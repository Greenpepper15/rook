/*
Copyright 2020 The Rook Authors. All rights reserved.

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

// Package zone to manage a rook object zone.
package zone

import (
	"context"
	"testing"
	"time"

	"github.com/coreos/pkg/capnslog"
	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	rookclient "github.com/rook/rook/pkg/client/clientset/versioned/fake"
	opcontroller "github.com/rook/rook/pkg/operator/ceph/controller"
	"github.com/rook/rook/pkg/operator/ceph/object"
	"github.com/rook/rook/pkg/operator/test"

	"github.com/rook/rook/pkg/clusterd"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	"github.com/rook/rook/pkg/operator/k8sutil"
	exectest "github.com/rook/rook/pkg/util/exec/test"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	zoneGroupGetJSON = `{
		"id": "fd8ff110-d3fd-49b4-b24f-f6cd3dddfedf",
		"name": "zonegroup-a",
		"api_name": "zonegroup-a",
		"is_master": true,
		"endpoints": [
			":80"
		],
		"hostnames": [],
		"hostnames_s3website": [],
		"master_zone": "6cb39d2c-3005-49da-9be3-c1a92a97d28a",
		"zones": [
			{
				"id": "6cb39d2c-3005-49da-9be3-c1a92a97d28a",
				"name": "zone-group",
				"endpoints": [
					":80"
				],
				"log_meta": "false",
				"log_data": "false",
				"bucket_index_max_shards": 0,
				"read_only": "false",
				"tier_type": "",
				"sync_from_all": "true",
				"sync_from": [],
				"redirect_zone": ""
			}
		],
		"placement_targets": [
			{
				"name": "default-placement",
				"tags": [],
				"storage_classes": [
					"STANDARD"
				]
			}
		],
		"default_placement": "default-placement",
		"realm_id": "237e6250-5f7d-4b85-9359-8cb2b1848507"
	}`
	zoneGetOutput  = `{"id": "test-id"}`
	zoneCreateJSON = `{
    		"id": "b1abbebb-e8ae-4c3b-880e-b009728bad53",
    		"name": "zone-a",
    		"domain_root": "zone-a.rgw.meta:root",
    		"control_pool": "zone-a.rgw.control",
    		"gc_pool": "zone-a.rgw.log:gc",
    		"lc_pool": "zone-a.rgw.log:lc",
    		"log_pool": "zone-a.rgw.log",
    		"intent_log_pool": "zone-a.rgw.log:intent",
    		"usage_log_pool": "zone-a.rgw.log:usage",
    		"roles_pool": "zone-a.rgw.meta:roles",
    		"reshard_pool": "zone-a.rgw.log:reshard",
    		"user_keys_pool": "zone-a.rgw.meta:users.keys",
    		"user_email_pool": "zone-a.rgw.meta:users.email",
    		"user_swift_pool": "zone-a.rgw.meta:users.swift",
    		"user_uid_pool": "zone-a.rgw.meta:users.uid",
    		"otp_pool": "zone-a.rgw.otp",
    		"system_key": {
        		"access_key": "",
        		"secret_key": ""
    		},
    		"placement_pools": [
        	    {
            		"key": "default-placement",
            		"val": {
                		"index_pool": "zone-a.rgw.buckets.index",
                		"storage_classes": {
                    		"STANDARD": {
                        		"data_pool": "zone-a.rgw.buckets.data"
                    		    }
                		},
                		"data_extra_pool": "zone-a.rgw.buckets.non-ec",
                		"index_type": 0
            		}
        	    }
    		],
    		"realm_id": "91b799b2-857d-4c96-8ade-5ceff7c8597e"
	}`
)

func TestCephObjectZoneController(t *testing.T) {
	ctx := context.TODO()
	capnslog.SetGlobalLogLevel(capnslog.DEBUG)
	name := "zone-a"
	zonegroup := "zonegroup-a"
	namespace := "rook-ceph"

	createPoolsCalled := false
	createObjectStorePoolsFunc = func(context *object.Context, cluster *cephv1.ClusterSpec, metadataPool, dataPool cephv1.PoolSpec) error {
		createPoolsCalled = true
		return nil
	}
	defer func() {
		createObjectStorePoolsFunc = object.CreateObjectStorePools
	}()

	commitChangesCalled := false
	commitConfigChangesFunc = func(c *object.Context) error {
		commitChangesCalled = true
		return nil
	}
	defer func() {
		commitConfigChangesFunc = object.CommitConfigChanges
	}()

	//
	// TEST 1 SETUP
	//
	// FAILURE: because no CephCluster
	//
	// A Pool resource with metadata and spec.
	metadataPool := cephv1.PoolSpec{}
	dataPool := cephv1.PoolSpec{}
	objectZone := &cephv1.CephObjectZone{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Generation: 0,
			Finalizers: []string{"cephobjectzone.ceph.rook.io"},
		},
		TypeMeta: metav1.TypeMeta{
			Kind: "CephObjectZone",
		},
		Spec: cephv1.ObjectZoneSpec{
			ZoneGroup:    zonegroup,
			MetadataPool: metadataPool,
			DataPool:     dataPool,
		},
	}

	// Objects to track in the fake client.
	object := []runtime.Object{
		objectZone,
	}

	executor := &exectest.MockExecutor{
		MockExecuteCommandWithOutput: func(command string, args ...string) (string, error) {
			if args[0] == "status" {
				return `{"fsid":"c47cac40-9bee-4d52-823b-ccd803ba5bfe","health":{"checks":{},"status":"HEALTH_ERR"},"pgmap":{"num_pgs":100,"pgs_by_state":[{"state_name":"active+clean","count":100}]}}`, nil
			}
			return "", nil
		},
	}

	clientset := test.New(t, 3)
	c := &clusterd.Context{
		Executor:      executor,
		RookClientset: rookclient.NewSimpleClientset(),
		Clientset:     clientset,
	}

	// Register operator types with the runtime scheme.
	s := scheme.Scheme
	s.AddKnownTypes(cephv1.SchemeGroupVersion, &cephv1.CephObjectZone{}, &cephv1.CephObjectZoneList{})

	// Create a fake client to mock API calls.
	cl := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(object...).Build()

	// Create a ReconcileObjectZone object with the scheme and fake client.
	clusterInfo := cephclient.AdminTestClusterInfo("rook")

	r := &ReconcileObjectZone{client: cl, scheme: s, context: c, clusterInfo: clusterInfo, recorder: events.NewFakeRecorder(50)}

	// Mock request to simulate Reconcile() being called on an event for a
	// watched resource .
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		},
	}

	res, err := r.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.True(t, res.Requeue)

	//
	// TEST 2:
	//
	// FAILURE: we have a cluster but it's not ready
	//
	cephCluster := &cephv1.CephCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespace,
			Namespace: namespace,
		},
		Status: cephv1.ClusterStatus{
			Phase: "",
			CephStatus: &cephv1.CephStatus{
				Health: "",
			},
		},
	}

	object = []runtime.Object{
		objectZone,
		cephCluster,
	}

	s.AddKnownTypes(cephv1.SchemeGroupVersion, &cephv1.CephObjectZone{}, &cephv1.CephObjectZoneList{}, &cephv1.CephCluster{}, &cephv1.CephClusterList{})
	cl = fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(object...).Build()

	// Create a ReconcileObjectZone object with the scheme and fake client.
	r = &ReconcileObjectZone{client: cl, scheme: r.scheme, context: r.context, recorder: events.NewFakeRecorder(50)}
	res, err = r.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.True(t, res.Requeue)

	//
	// TEST 3:
	//
	// Failure: The CephCluster is ready but no ObjectZoneGroup has been created
	//

	// Mock clusterInfo
	secrets := map[string][]byte{
		"fsid":         []byte(name),
		"mon-secret":   []byte("monsecret"),
		"admin-secret": []byte("adminsecret"),
	}
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rook-ceph-mon",
			Namespace: namespace,
		},
		Data: secrets,
		Type: k8sutil.RookType,
	}
	_, err = r.context.Clientset.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
	assert.NoError(t, err)

	// Add ready status to the CephCluster
	cephCluster.Status.Phase = k8sutil.ReadyStatus
	cephCluster.Status.CephStatus.Health = "HEALTH_OK"

	// Create a fake client to mock API calls.
	cl = fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(object...).Build()

	executor = &exectest.MockExecutor{
		MockExecuteCommandWithOutput: func(command string, args ...string) (string, error) {
			if args[0] == "status" {
				return `{"fsid":"c47cac40-9bee-4d52-823b-ccd803ba5bfe","health":{"checks":{},"status":"HEALTH_OK"},"pgmap":{"num_pgs":100,"pgs_by_state":[{"state_name":"active+clean","count":100}]}}`, nil
			}
			return "", nil
		},
		MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
			if args[0] == "zonegroup" && args[1] == "get" {
				return zoneGroupGetJSON, nil
			}
			return "", nil
		},
	}
	r.context.Executor = executor

	r = &ReconcileObjectZone{client: cl, scheme: r.scheme, context: r.context, recorder: events.NewFakeRecorder(50)}

	res, err = r.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.True(t, res.Requeue)

	//
	// TEST 4:
	//
	// Success: The CephCluster is ready and ObjectZone has been created
	//

	objectZoneGroup := &cephv1.CephObjectZoneGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      zonegroup,
			Namespace: namespace,
		},
		TypeMeta: metav1.TypeMeta{
			Kind: "CephObjectZoneGroup",
		},
		Spec: cephv1.ObjectZoneGroupSpec{},
	}

	// Objects to track in the fake client.
	object = []runtime.Object{
		objectZone,
		objectZoneGroup,
		cephCluster,
	}

	executor = &exectest.MockExecutor{
		MockExecuteCommandWithOutput: func(command string, args ...string) (string, error) {
			if args[0] == "status" {
				return `{"fsid":"c47cac40-9bee-4d52-823b-ccd803ba5bfe","health":{"checks":{},"status":"HEALTH_ERR"},"pgmap":{"num_pgs":100,"pgs_by_state":[{"state_name":"active+clean","count":100}]}}`, nil
			}
			return "", nil
		},
		MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
			if args[0] == "zonegroup" && args[1] == "get" {
				return zoneGroupGetJSON, nil
			}
			if args[0] == "zone" && args[1] == "get" {
				return zoneGetOutput, nil
			}
			if args[0] == "zone" && args[1] == "create" {
				return zoneCreateJSON, nil
			}
			return "", nil
		},
	}

	clientset = test.New(t, 3)
	c = &clusterd.Context{
		Executor:      executor,
		RookClientset: rookclient.NewSimpleClientset(),
		Clientset:     clientset,
	}

	_, err = c.Clientset.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
	assert.NoError(t, err)

	s.AddKnownTypes(cephv1.SchemeGroupVersion, &cephv1.CephObjectZoneGroup{}, &cephv1.CephObjectZoneGroupList{}, &cephv1.CephCluster{}, &cephv1.CephClusterList{}, &cephv1.CephObjectZone{}, &cephv1.CephObjectZoneList{})

	cl = fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(object...).Build()

	r = &ReconcileObjectZone{client: cl, scheme: s, context: c, clusterInfo: clusterInfo, recorder: events.NewFakeRecorder(50)}

	err = r.client.Get(context.TODO(), types.NamespacedName{Name: zonegroup, Namespace: namespace}, objectZoneGroup)
	assert.NoError(t, err, objectZoneGroup)

	req = reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		},
	}

	assert.False(t, createPoolsCalled)
	assert.False(t, commitChangesCalled)
	res, err = r.Reconcile(ctx, req)
	assert.NoError(t, err)
	assert.False(t, res.Requeue)
	err = r.client.Get(context.TODO(), req.NamespacedName, objectZone)
	assert.NoError(t, err)
	assert.True(t, createPoolsCalled)
	assert.True(t, commitChangesCalled)
}

func TestGetRootPoolNamespace(t *testing.T) {
	namespace := "rook-ceph"
	s := scheme.Scheme
	s.AddKnownTypes(cephv1.SchemeGroupVersion, &cephv1.CephObjectRealm{}, &cephv1.CephObjectRealmList{})

	realm := &cephv1.CephObjectRealm{
		ObjectMeta: metav1.ObjectMeta{Name: "realm-a", Namespace: namespace},
		Spec:       cephv1.ObjectRealmSpec{IsolatedRootPool: true},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(realm).Build()
	r := &ReconcileObjectZone{client: cl, scheme: s, opManagerContext: context.TODO()}

	t.Run("isolated realm", func(t *testing.T) {
		ns, err := r.getRootPoolNamespace(namespace, "realm-a")
		assert.NoError(t, err)
		assert.Equal(t, "realm-a", ns)
	})

	t.Run("empty realm name uses the shared root", func(t *testing.T) {
		ns, err := r.getRootPoolNamespace(namespace, "")
		assert.NoError(t, err)
		assert.Equal(t, "", ns)
	})

	t.Run("realm without isolatedRootPool", func(t *testing.T) {
		realm.Spec.IsolatedRootPool = false
		cl := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(realm).Build()
		r := &ReconcileObjectZone{client: cl, scheme: s, opManagerContext: context.TODO()}
		ns, err := r.getRootPoolNamespace(namespace, "realm-a")
		assert.NoError(t, err)
		assert.Equal(t, "", ns)
	})

	t.Run("missing realm CR", func(t *testing.T) {
		ns, err := r.getRootPoolNamespace(namespace, "no-such-realm")
		assert.Error(t, err)
		assert.Equal(t, "", ns)
	})
}

// TestReconcileZoneDeletionWithMissingRealm covers zone deletion when the CephObjectRealm CR is
// already gone: realm CRs carry no finalizer and can disappear before the finalizer-gated zone
// finishes deleting. The reconciler must recover the `.rgw.root` namespace recorded on the zone
// (falling back to the shared root for zones without a record) and run the normal deletion path,
// so cleanup targets the zone's actual root pool view and the dependents guard still applies.
func TestReconcileZoneDeletionWithMissingRealm(t *testing.T) {
	name := "zone-a"
	zonegroup := "zonegroup-a"
	realmName := "realm-a"
	namespace := "rook-ceph"
	// A zonegroup whose zone list contains zone-a, to drive the zonePresent=true dependents path.
	zoneGroupContainsZoneJSON := `{"id": "fd8ff110", "name": "zonegroup-a", "master_zone": "other-zone-id", "zones": [{"id": "zone-a-id", "name": "zone-a", "endpoints": [":80"]}]}`

	s := scheme.Scheme
	s.AddKnownTypes(cephv1.SchemeGroupVersion,
		&cephv1.CephObjectZone{}, &cephv1.CephObjectZoneList{},
		&cephv1.CephObjectZoneGroup{}, &cephv1.CephObjectZoneGroupList{},
		&cephv1.CephObjectRealm{}, &cephv1.CephObjectRealmList{},
		&cephv1.CephCluster{}, &cephv1.CephClusterList{})

	// run reconciles one zone deletion with the realm CR absent and returns the outcome.
	run := func(t *testing.T, annotations map[string]string, zoneGroupJSON string, dependentStore *cephv1.CephObjectStore) (reconcile.Result, [][]string, *cephv1.CephObjectZone, error) {
		ctx := context.TODO()
		objectZone := &cephv1.CephObjectZone{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   namespace,
				Annotations: annotations,
				// The production finalizer plus the finalizer name the reconciler computes under
				// the fake client, which strips TypeMeta on Get so buildFinalizerName sees an empty
				// Kind. Seeding both keeps AddFinalizerIfNotPresent a no-op (a real API server
				// rejects adding finalizers to a deleting object) and makes RemoveFinalizer
				// observable: successful deletion removes the computed ".ceph.rook.io" entry.
				Finalizers: []string{"cephobjectzone.ceph.rook.io", ".ceph.rook.io"},
			},
			TypeMeta: metav1.TypeMeta{Kind: "CephObjectZone"},
			Spec: cephv1.ObjectZoneSpec{
				ZoneGroup:    zonegroup,
				MetadataPool: cephv1.PoolSpec{},
				DataPool:     cephv1.PoolSpec{},
			},
		}
		// The zonegroup CR still exists and points at a realm whose CR has been deleted.
		objectZoneGroup := &cephv1.CephObjectZoneGroup{
			ObjectMeta: metav1.ObjectMeta{Name: zonegroup, Namespace: namespace},
			TypeMeta:   metav1.TypeMeta{Kind: "CephObjectZoneGroup"},
			Spec:       cephv1.ObjectZoneGroupSpec{Realm: realmName},
		}
		cephCluster := &cephv1.CephCluster{
			ObjectMeta: metav1.ObjectMeta{Name: namespace, Namespace: namespace},
			Status: cephv1.ClusterStatus{
				Phase:      k8sutil.ReadyStatus,
				CephStatus: &cephv1.CephStatus{Health: "HEALTH_OK"},
			},
		}

		var radosgwCalls [][]string
		executor := &exectest.MockExecutor{
			MockExecuteCommandWithOutput: func(command string, args ...string) (string, error) {
				if args[0] == "status" {
					return `{"fsid":"c47cac40-9bee-4d52-823b-ccd803ba5bfe","health":{"checks":{},"status":"HEALTH_OK"},"pgmap":{"num_pgs":100,"pgs_by_state":[{"state_name":"active+clean","count":100}]}}`, nil
				}
				return "", nil
			},
			MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
				radosgwCalls = append(radosgwCalls, args)
				if args[0] == "zonegroup" && args[1] == "get" {
					return zoneGroupJSON, nil
				}
				if args[0] == "zone" && args[1] == "get" {
					return zoneGetOutput, nil
				}
				return "", nil
			},
		}

		rookObjects := []runtime.Object{}
		if dependentStore != nil {
			rookObjects = append(rookObjects, dependentStore)
		}
		c := &clusterd.Context{
			Executor:      executor,
			RookClientset: rookclient.NewSimpleClientset(rookObjects...),
			Clientset:     test.New(t, 3),
		}
		secret := &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "rook-ceph-mon", Namespace: namespace},
			Data: map[string][]byte{
				"fsid":         []byte(name),
				"mon-secret":   []byte("monsecret"),
				"admin-secret": []byte("adminsecret"),
			},
			Type: k8sutil.RookType,
		}
		_, err := c.Clientset.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
		assert.NoError(t, err)

		// No CephObjectRealm object is seeded on purpose: the realm CR is gone.
		cl := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(objectZone, objectZoneGroup, cephCluster).Build()
		r := &ReconcileObjectZone{
			client:           cl,
			scheme:           s,
			context:          c,
			clusterInfo:      cephclient.AdminTestClusterInfo("rook"),
			opManagerContext: ctx,
			recorder:         events.NewFakeRecorder(50),
		}

		// Deleting a finalizer-bearing object keeps it around with the deletion timestamp set.
		err = cl.Delete(ctx, objectZone)
		assert.NoError(t, err)

		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: namespace}}
		res, err := r.Reconcile(ctx, req)

		remaining := &cephv1.CephObjectZone{}
		getErr := cl.Get(ctx, req.NamespacedName, remaining)
		assert.NoError(t, getErr)
		return res, radosgwCalls, remaining, err
	}

	rootPoolFlag := "--rgw-zone-root-pool=.rgw.root:" + realmName

	t.Run("recorded namespace routes cleanup to the isolated root", func(t *testing.T) {
		res, calls, remaining, err := run(t, map[string]string{rootPoolNamespaceAnnotation: realmName}, zoneGroupGetJSON, nil)
		assert.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, res)
		assert.NotEmpty(t, calls)
		for _, args := range calls {
			assert.Contains(t, args, rootPoolFlag, "radosgw-admin call %v must target the recorded root pool namespace", args)
		}
		// Successful deletion removed the finalizer the reconciler manages (the computed name,
		// see the seeding comment above); only the production finalizer remains.
		assert.Equal(t, []string{"cephobjectzone.ceph.rook.io"}, remaining.Finalizers)
	})

	t.Run("no recorded namespace falls back to the shared root", func(t *testing.T) {
		res, calls, remaining, err := run(t, nil, zoneGroupGetJSON, nil)
		assert.NoError(t, err)
		assert.Equal(t, reconcile.Result{}, res)
		assert.NotEmpty(t, calls, "shared-root cleanup must still run for zones without a recorded namespace")
		for _, args := range calls {
			for _, arg := range args {
				assert.NotContains(t, arg, "-root-pool=", "radosgw-admin call %v must target the shared root", args)
			}
		}
		assert.Equal(t, []string{"cephobjectzone.ceph.rook.io"}, remaining.Finalizers)
	})

	t.Run("dependent stores still block deletion", func(t *testing.T) {
		dependentStore := &cephv1.CephObjectStore{
			ObjectMeta: metav1.ObjectMeta{Name: "store-a", Namespace: namespace},
			Spec:       cephv1.ObjectStoreSpec{Zone: cephv1.ZoneSpec{Name: name}},
		}
		res, calls, remaining, _ := run(t, map[string]string{rootPoolNamespaceAnnotation: realmName}, zoneGroupContainsZoneJSON, dependentStore)
		assert.Equal(t, opcontroller.WaitForRequeueIfFinalizerBlocked, res)
		for _, args := range calls {
			assert.NotEqual(t, "delete", args[1], "no deletion command may run while a store depends on the zone: %v", args)
			assert.NotEqual(t, "remove", args[1], "no removal command may run while a store depends on the zone: %v", args)
		}
		// The finalizer must survive so the zone cannot vanish under the dependent store.
		assert.ElementsMatch(t, []string{"cephobjectzone.ceph.rook.io", ".ceph.rook.io"}, remaining.Finalizers)
	})

	// The annotation lives on a user-mutable CR and its value is spliced into radosgw-admin
	// arguments where `:` changes Ceph's pool[:namespace] parsing — a tampered value must stop
	// the deletion with an actionable error, not run cleanup against a mangled location.
	t.Run("tampered annotation fails the deletion closed", func(t *testing.T) {
		res, calls, remaining, err := run(t, map[string]string{rootPoolNamespaceAnnotation: "evil:pool"}, zoneGroupGetJSON, nil)
		// ReportReconcileResult consumes the validation error into a ReconcileFailed Warning
		// event plus this requeue result, so the reconcile error itself is nil by convention.
		assert.NoError(t, err)
		assert.Equal(t, waitForRequeueIfObjectZoneGroupNotReady, res)
		assert.Empty(t, calls, "no radosgw-admin command may run with an invalid root pool namespace")
		// The finalizer must survive so the zone is not released half-cleaned.
		assert.ElementsMatch(t, []string{"cephobjectzone.ceph.rook.io", ".ceph.rook.io"}, remaining.Finalizers)
	})
}

// TestPersistRootPoolNamespace covers the annotation write policy: isolated zones are stamped
// before any Ceph-side work, zones on the shared root are left untouched (an absent annotation
// already means the shared root to the deletion fallback, and stamping every pre-existing zone
// would churn user-owned metadata).
func TestPersistRootPoolNamespace(t *testing.T) {
	ctx := context.TODO()
	s := scheme.Scheme
	s.AddKnownTypes(cephv1.SchemeGroupVersion, &cephv1.CephObjectZone{})

	zone := &cephv1.CephObjectZone{
		ObjectMeta: metav1.ObjectMeta{Name: "zone-a", Namespace: "rook-ceph"},
		TypeMeta:   metav1.TypeMeta{Kind: "CephObjectZone"},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(zone).Build()
	r := &ReconcileObjectZone{client: cl, scheme: s, opManagerContext: ctx}
	nsName := types.NamespacedName{Name: "zone-a", Namespace: "rook-ceph"}

	// shared-root zone: no annotation written
	assert.NoError(t, r.persistRootPoolNamespace(zone, ""))
	fetched := &cephv1.CephObjectZone{}
	assert.NoError(t, cl.Get(ctx, nsName, fetched))
	_, ok := fetched.Annotations[rootPoolNamespaceAnnotation]
	assert.False(t, ok, "zones using the shared root must not be stamped")

	// isolated zone: annotation recorded
	assert.NoError(t, r.persistRootPoolNamespace(zone, "realm-a"))
	assert.NoError(t, cl.Get(ctx, nsName, fetched))
	assert.Equal(t, "realm-a", fetched.Annotations[rootPoolNamespaceAnnotation])

	// unchanged value: idempotent
	assert.NoError(t, r.persistRootPoolNamespace(zone, "realm-a"))
	assert.NoError(t, cl.Get(ctx, nsName, fetched))
	assert.Equal(t, "realm-a", fetched.Annotations[rootPoolNamespaceAnnotation])
}
