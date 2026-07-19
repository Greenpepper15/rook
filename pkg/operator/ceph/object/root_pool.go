/*
Copyright 2026 The Rook Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package object

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"syscall"

	"github.com/pkg/errors"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	opcontroller "github.com/rook/rook/pkg/operator/ceph/controller"
	"github.com/rook/rook/pkg/util/exec"
	"github.com/rook/rook/pkg/util/log"
	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
)

// RGW keeps its realm/zonegroup/zone/period records in the pools named by the four
// rgw_*_root_pool config options, all defaulting to the shared `.rgw.root`. Ceph parses these
// values as `pool[:namespace]`, so pointing all four at `.rgw.root:<realm>` confines a realm's
// topology records to their own RADOS namespace (spec isolatedRootPool). The namespace is
// derived purely from the CR spec and pushed to every consumer — radosgw-admin invocations via
// Context.RootPoolNamespace and the RGW daemon via its pod command line — it is never stored in
// or read from Ceph. The namespace value is a Kubernetes resource name, so it can never contain
// the `:`/`\` characters that are special in the pool:namespace syntax.

// RootPoolNamespaceForRealm returns the `.rgw.root` RADOS namespace holding the realm's
// topology records, or "" when the realm uses the shared un-namespaced root pool.
func RootPoolNamespaceForRealm(realm *cephv1.CephObjectRealm) string {
	if realm.Spec.IsolatedRootPool {
		return realm.Name
	}
	return ""
}

// rootPoolOptionNames are the four config options through which RGW resolves the pool holding
// each topology record type; Ceph parses their values as `pool[:namespace]`.
var rootPoolOptionNames = []string{
	"rgw_realm_root_pool",
	"rgw_zonegroup_root_pool",
	"rgw_zone_root_pool",
	"rgw_period_root_pool",
}

// rootPoolArgs returns the CLI overrides pointing every RGW topology record type at the realm's
// RADOS namespace within `.rgw.root`. The same --key=value form is understood by radosgw-admin
// and by the radosgw daemon.
func rootPoolArgs(rootPoolNamespace string) []string {
	val := rootPool + ":" + rootPoolNamespace
	args := make([]string, 0, len(rootPoolOptionNames))
	for _, option := range rootPoolOptionNames {
		args = append(args, "--"+strings.ReplaceAll(option, "_", "-")+"="+val)
	}
	return args
}

// rootPoolMonConfigOptions returns the same root pool overrides in mon config store form. They
// are written to the RGW daemon's config section as a backstop for operator downgrades: the pod
// CLI args outrank the mon config database while they are present, but a pod template rendered
// by an older Rook operator carries no root-pool flags, and without these keys its daemons would
// resolve the shared `.rgw.root` and attach to whatever topology that operator (re-)creates
// there. The keys share the daemon config section's lifecycle (removed with the daemon).
func rootPoolMonConfigOptions(rootPoolNamespace string) map[string]string {
	val := rootPool + ":" + rootPoolNamespace
	options := make(map[string]string, len(rootPoolOptionNames))
	for _, option := range rootPoolOptionNames {
		options[option] = val
	}
	return options
}

// validateIsolatedRootPool checks the CephObjectStore constraints for spec.isolatedRootPool.
// The CRD CEL rules enforce the zone constraint at admission; this also guards clusters running
// with an older CRD.
func validateIsolatedRootPool(spec *cephv1.ObjectStoreSpec) error {
	if !spec.IsolatedRootPool {
		return nil
	}
	if spec.IsMultisite() {
		return fmt.Errorf("isolatedRootPool must not be set when zone.name is set (multisite); set isolatedRootPool on the CephObjectRealm instead")
	}
	if IsNeedToCreateObjectStorePools(spec.SharedPools) {
		return fmt.Errorf("isolatedRootPool requires sharedPools (metadataPoolName/dataPoolName or a default pool placement)")
	}
	return nil
}

// canDeleteRootPool reports whether deleting the shared `.rgw.root` pool is safe while tearing
// down this object store. The lastStore signal is computed from `realm list`, which sees only
// the caller's own root-pool namespace: a store on the shared (default) namespace cannot see
// realms isolated into RADOS namespaces, and an isolated store sees nothing but itself.
// Deleting the pool destroys every namespace in it, so require that no namespace other than the
// caller's own still holds objects. Records remaining in the caller's own namespace (period and
// default-marker residue left behind by deleteRealm) never block deletion — removing the pool
// is how that residue is cleaned up for the last store.
func canDeleteRootPool(objContext *Context) bool {
	namespaces, err := cephclient.RadosNamespacesWithObjects(objContext.Context, objContext.clusterInfo, rootPool)
	if err != nil {
		log.NamedWarning(objContext.NsName(), logger, "not deleting pool %q: cannot verify it is unused by other realms. %v", rootPool, err)
		return false
	}

	inUseBy := []string{}
	for _, namespace := range namespaces {
		if namespace == objContext.RootPoolNamespace {
			continue
		}
		if namespace == "" {
			namespace = "<default>"
		}
		inUseBy = append(inUseBy, namespace)
	}
	if len(inUseBy) > 0 {
		log.NamedInfo(objContext.NsName(), logger, "not deleting pool %q: rados namespaces %v still hold rgw topology records that are not visible to this store's realm list", rootPool, inUseBy)
		return false
	}
	return true
}

// CheckRealmLocationConflict fails when a realm about to be created or pulled at the root-pool
// location implied by the CR spec — the shared un-namespaced `.rgw.root`, or the realm's RADOS
// namespace when isolatedRootPool is set — already has records at the opposite location:
// radosgw-admin cannot relocate topology, and proceeding would fork the realm's identity (new
// UUIDs) away from the records referenced by existing zonegroups, zones, periods, and bucket and
// user metadata. The namespace key is the realm name by convention, so the opposite location is
// always derivable from the CR alone. Callers run this only on the about-to-create path, so it
// costs nothing in steady state. Deleting and re-creating a CR with a different isolatedRootPool
// value (the CEL immutability rule only guards updates) and an outdated CRD pruning the field
// both funnel into this refusal instead of silently forking the realm.
func CheckRealmLocationConflict(objContext *Context, realmName string) error {
	otherCtx := *objContext
	otherLocation := "the shared .rgw.root pool"
	if objContext.RootPoolNamespace == "" {
		otherCtx.RootPoolNamespace = realmName
		otherLocation = fmt.Sprintf("RADOS namespace %q of the .rgw.root pool", realmName)
	} else {
		otherCtx.RootPoolNamespace = ""
	}

	_, err := RunAdminCommandNoMultisite(&otherCtx, true, "realm", "get", "--rgw-realm="+realmName)
	if err == nil {
		if objContext.RootPoolNamespace == "" {
			return errors.Errorf("realm %q already has records in %s: existing topology cannot be relocated; re-create the CR with isolatedRootPool: true to adopt the records, or remove them", realmName, otherLocation)
		}
		return errors.Errorf("realm %q already exists in the shared .rgw.root pool: isolatedRootPool applies only to newly created realms/object stores, existing topology cannot be relocated", realmName)
	}
	// the pod used to exec the command (act as a proxy) is not found/ready yet; let the caller requeue
	if kerrors.IsNotFound(err) {
		return err
	}
	if code, extractErr := exec.ExtractExitCode(err); extractErr == nil && code == int(syscall.ENOENT) {
		return nil
	}
	return errors.Wrapf(err, "failed to check whether realm %q exists in %s", realmName, otherLocation)
}

// Names of the CRDs whose schemas must contain spec.isolatedRootPool for the field to survive
// admission.
const (
	ObjectStoreCRDName = "cephobjectstores.ceph.rook.io"
	ObjectRealmCRDName = "cephobjectrealms.ceph.rook.io"
)

// isolatedRootPoolCRDSupported caches POSITIVE schema-check results per CRD name for the
// operator's lifetime. Negative results are not cached, so applying updated CRDs clears the
// warning on the next reconcile without an operator restart.
var isolatedRootPoolCRDSupported sync.Map

// WarnIfCRDPrunesIsolatedRootPool warns — in the operator log and with a Warning event on the
// object being reconciled — when the installed CRD's schema predates spec.isolatedRootPool. The
// API server silently prunes fields unknown to the schema, so on such a cluster a user setting
// isolatedRootPool gets an un-isolated store or realm with no error anywhere; the operator
// cannot recover the pruned field (absent and never-set are indistinguishable in the stored
// object), so it warns about the skew condition itself. Failures to read the CRD (e.g.
// restricted RBAC) fail open: a diagnostic must never block reconciliation.
func WarnIfCRDPrunesIsolatedRootPool(ctx context.Context, clusterdContext *clusterd.Context, recorder events.EventRecorder, obj runtime.Object, namespace, name, crdName string) {
	if crdSupportsIsolatedRootPool(ctx, clusterdContext, crdName) {
		return
	}
	msg := fmt.Sprintf("the installed CRD %q predates spec.isolatedRootPool: if the field was set, the API server silently dropped it at admission; apply the updated CRDs (before the operator, per the standard upgrade order) for it to take effect", crdName)
	log.NamedWarning(opcontroller.NsName(namespace, name), logger, "%s", msg)
	if recorder != nil {
		recorder.Eventf(obj, nil, v1.EventTypeWarning, "OutdatedCRD", "ReconcileStarted", "%s", msg)
	}
}

func crdSupportsIsolatedRootPool(ctx context.Context, clusterdContext *clusterd.Context, crdName string) bool {
	if _, ok := isolatedRootPoolCRDSupported.Load(crdName); ok {
		return true
	}
	if clusterdContext.ApiExtensionsClient == nil {
		// contexts without a CRD client (external tooling, tests) cannot be checked; fail open
		return true
	}
	crd, err := clusterdContext.ApiExtensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		logger.Debugf("skipping the isolatedRootPool schema check: failed to get CRD %q. %v", crdName, err)
		return true
	}
	for i := range crd.Spec.Versions {
		version := &crd.Spec.Versions[i]
		if !version.Served || version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
			continue
		}
		spec, ok := version.Schema.OpenAPIV3Schema.Properties["spec"]
		if !ok {
			continue
		}
		if _, ok := spec.Properties["isolatedRootPool"]; ok {
			isolatedRootPoolCRDSupported.Store(crdName, struct{}{})
			return true
		}
	}
	return false
}
