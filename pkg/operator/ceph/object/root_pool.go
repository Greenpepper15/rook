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
	"fmt"
	"syscall"

	"github.com/pkg/errors"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	"github.com/rook/rook/pkg/util/exec"
	"github.com/rook/rook/pkg/util/log"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
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

// rootPoolArgs returns the CLI overrides pointing every RGW topology record type at the realm's
// RADOS namespace within `.rgw.root`. The same --key=value form is understood by radosgw-admin
// and by the radosgw daemon.
func rootPoolArgs(rootPoolNamespace string) []string {
	val := rootPool + ":" + rootPoolNamespace
	return []string{
		"--rgw-realm-root-pool=" + val,
		"--rgw-zonegroup-root-pool=" + val,
		"--rgw-zone-root-pool=" + val,
		"--rgw-period-root-pool=" + val,
	}
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

// CheckRealmNotInSharedRoot fails when a realm configured for an isolated root pool already has
// records in the shared un-namespaced `.rgw.root`: radosgw-admin cannot relocate topology, and
// re-creating it in the namespace would fork the realm's identity (new UUIDs) away from the
// records referenced by existing bucket and user metadata. Callers run this only on the
// about-to-create path, so it costs nothing in steady state.
func CheckRealmNotInSharedRoot(objContext *Context, realmName string) error {
	sharedCtx := *objContext
	sharedCtx.RootPoolNamespace = ""
	_, err := RunAdminCommandNoMultisite(&sharedCtx, true, "realm", "get", "--rgw-realm="+realmName)
	if err == nil {
		return errors.Errorf("realm %q already exists in the shared .rgw.root pool: isolatedRootPool applies only to newly created realms/object stores, existing topology cannot be relocated", realmName)
	}
	// the pod used to exec the command (act as a proxy) is not found/ready yet; let the caller requeue
	if kerrors.IsNotFound(err) {
		return err
	}
	if code, extractErr := exec.ExtractExitCode(err); extractErr == nil && code == int(syscall.ENOENT) {
		return nil
	}
	return errors.Wrapf(err, "failed to check whether realm %q exists in the shared .rgw.root pool", realmName)
}
