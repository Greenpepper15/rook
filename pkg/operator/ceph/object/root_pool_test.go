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
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"

	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	rookfake "github.com/rook/rook/pkg/client/clientset/versioned/fake"
	"github.com/rook/rook/pkg/clusterd"
	cephclient "github.com/rook/rook/pkg/daemon/ceph/client"
	exectest "github.com/rook/rook/pkg/util/exec/test"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kexec "k8s.io/utils/exec"
)

func TestRootPoolNamespaceForRealm(t *testing.T) {
	realm := &cephv1.CephObjectRealm{ObjectMeta: metav1.ObjectMeta{Name: "realm-a"}}
	assert.Equal(t, "", RootPoolNamespaceForRealm(realm))

	realm.Spec.IsolatedRootPool = true
	assert.Equal(t, "realm-a", RootPoolNamespaceForRealm(realm))
}

func TestRootPoolArgs(t *testing.T) {
	assert.Equal(t, []string{
		"--rgw-realm-root-pool=.rgw.root:my-store",
		"--rgw-zonegroup-root-pool=.rgw.root:my-store",
		"--rgw-zone-root-pool=.rgw.root:my-store",
		"--rgw-period-root-pool=.rgw.root:my-store",
	}, rootPoolArgs("my-store"))
}

func TestValidateIsolatedRootPool(t *testing.T) {
	sharedPools := cephv1.ObjectSharedPoolsSpec{MetadataPoolName: "meta-pool", DataPoolName: "data-pool"}

	t.Run("disabled needs nothing", func(t *testing.T) {
		assert.NoError(t, validateIsolatedRootPool(&cephv1.ObjectStoreSpec{}))
	})
	t.Run("enabled with shared pools", func(t *testing.T) {
		spec := &cephv1.ObjectStoreSpec{IsolatedRootPool: true, SharedPools: sharedPools}
		assert.NoError(t, validateIsolatedRootPool(spec))
	})
	t.Run("enabled with default pool placement", func(t *testing.T) {
		spec := &cephv1.ObjectStoreSpec{
			IsolatedRootPool: true,
			SharedPools: cephv1.ObjectSharedPoolsSpec{
				PoolPlacements: []cephv1.PoolPlacementSpec{{Name: "default", Default: true, MetadataPoolName: "meta-pool", DataPoolName: "data-pool"}},
			},
		}
		assert.NoError(t, validateIsolatedRootPool(spec))
	})
	t.Run("enabled without shared pools", func(t *testing.T) {
		spec := &cephv1.ObjectStoreSpec{IsolatedRootPool: true}
		err := validateIsolatedRootPool(spec)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "requires sharedPools")
	})
	t.Run("enabled with zone reference", func(t *testing.T) {
		spec := &cephv1.ObjectStoreSpec{IsolatedRootPool: true, SharedPools: sharedPools, Zone: cephv1.ZoneSpec{Name: "zone-a"}}
		err := validateIsolatedRootPool(spec)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CephObjectRealm")
	})
}

func TestCheckRealmLocationConflict(t *testing.T) {
	newContext := func(executor *exectest.MockExecutor, rootPoolNamespace string) *Context {
		return &Context{
			Context:           &clusterd.Context{Executor: executor},
			clusterInfo:       cephclient.AdminTestClusterInfo("mycluster"),
			Name:              "my-store",
			Realm:             "my-store",
			RootPoolNamespace: rootPoolNamespace,
		}
	}
	hasRootPoolFlag := func(args []string) bool {
		for _, arg := range args {
			if strings.HasPrefix(arg, "--rgw-realm-root-pool=") {
				return true
			}
		}
		return false
	}
	enoent := func() error {
		return kexec.CodeExitError{Err: errors.New("exit status 2"), Code: int(syscall.ENOENT)}
	}

	t.Run("isolated realm absent from shared root", func(t *testing.T) {
		var captured []string
		executor := &exectest.MockExecutor{
			MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
				captured = args
				return "", enoent()
			},
		}
		err := CheckRealmLocationConflict(newContext(executor, "my-store"), "my-store")
		assert.NoError(t, err)
		// the check must look at the shared, un-namespaced root pool
		assert.False(t, hasRootPoolFlag(captured))
		assert.Contains(t, captured, "realm")
		assert.Contains(t, captured, "get")
		assert.Contains(t, captured, "--rgw-realm=my-store")
	})

	t.Run("isolated realm present in shared root", func(t *testing.T) {
		executor := &exectest.MockExecutor{
			MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
				return `{"id": "91b799b2-857d-4c96-8ade-5ceff7c8597e", "name": "my-store"}`, nil
			},
		}
		err := CheckRealmLocationConflict(newContext(executor, "my-store"), "my-store")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists in the shared .rgw.root")
	})

	t.Run("shared realm absent from its RADOS namespace", func(t *testing.T) {
		var captured []string
		executor := &exectest.MockExecutor{
			MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
				captured = args
				return "", enoent()
			},
		}
		err := CheckRealmLocationConflict(newContext(executor, ""), "my-store")
		assert.NoError(t, err)
		// the check must look at the realm's RADOS namespace of the root pool
		assert.Contains(t, captured, "--rgw-realm-root-pool=.rgw.root:my-store")
		assert.Contains(t, captured, "realm")
		assert.Contains(t, captured, "get")
		assert.Contains(t, captured, "--rgw-realm=my-store")
	})

	t.Run("shared realm present in its RADOS namespace", func(t *testing.T) {
		executor := &exectest.MockExecutor{
			MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
				return `{"id": "91b799b2-857d-4c96-8ade-5ceff7c8597e", "name": "my-store"}`, nil
			},
		}
		err := CheckRealmLocationConflict(newContext(executor, ""), "my-store")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `already has records in RADOS namespace "my-store"`)
		assert.Contains(t, err.Error(), "isolatedRootPool: true")
	})

	t.Run("unexpected error", func(t *testing.T) {
		executor := &exectest.MockExecutor{
			MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
				return "", kexec.CodeExitError{Err: errors.New("exit status 5"), Code: 5}
			},
		}
		err := CheckRealmLocationConflict(newContext(executor, "my-store"), "my-store")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check whether realm")
	})
}

func TestGetMultisiteForObjectStoreRootPoolNamespace(t *testing.T) {
	ctx := context.TODO()
	namespace := "rook-ceph"

	t.Run("non-multisite store without isolatedRootPool", func(t *testing.T) {
		spec := &cephv1.ObjectStoreSpec{}
		realm, zonegroup, zone, rootPoolNamespace, err := getMultisiteForObjectStore(ctx, &clusterd.Context{}, spec, namespace, "my-store")
		assert.NoError(t, err)
		assert.Equal(t, "my-store", realm)
		assert.Equal(t, "my-store", zonegroup)
		assert.Equal(t, "my-store", zone)
		assert.Equal(t, "", rootPoolNamespace)
	})

	t.Run("non-multisite store with isolatedRootPool", func(t *testing.T) {
		spec := &cephv1.ObjectStoreSpec{IsolatedRootPool: true}
		_, _, _, rootPoolNamespace, err := getMultisiteForObjectStore(ctx, &clusterd.Context{}, spec, namespace, "my-store")
		assert.NoError(t, err)
		assert.Equal(t, "my-store", rootPoolNamespace)
	})

	t.Run("external store", func(t *testing.T) {
		spec := &cephv1.ObjectStoreSpec{Gateway: cephv1.GatewaySpec{ExternalRgwEndpoints: []cephv1.EndpointAddress{{IP: "192.168.0.1"}}}}
		_, _, _, rootPoolNamespace, err := getMultisiteForObjectStore(ctx, &clusterd.Context{}, spec, namespace, "my-store")
		assert.NoError(t, err)
		assert.Equal(t, "", rootPoolNamespace)
	})

	t.Run("multisite store follows the realm setting", func(t *testing.T) {
		zone := &cephv1.CephObjectZone{
			ObjectMeta: metav1.ObjectMeta{Name: "zone-a", Namespace: namespace},
			Spec:       cephv1.ObjectZoneSpec{ZoneGroup: "zonegroup-a"},
		}
		zonegroup := &cephv1.CephObjectZoneGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "zonegroup-a", Namespace: namespace},
			Spec:       cephv1.ObjectZoneGroupSpec{Realm: "realm-a"},
		}
		realm := &cephv1.CephObjectRealm{
			ObjectMeta: metav1.ObjectMeta{Name: "realm-a", Namespace: namespace},
			Spec:       cephv1.ObjectRealmSpec{IsolatedRootPool: true},
		}
		clusterdCtx := &clusterd.Context{RookClientset: rookfake.NewSimpleClientset(zone, zonegroup, realm)}

		spec := &cephv1.ObjectStoreSpec{Zone: cephv1.ZoneSpec{Name: "zone-a"}}
		realmName, zonegroupName, zoneName, rootPoolNamespace, err := getMultisiteForObjectStore(ctx, clusterdCtx, spec, namespace, "my-store")
		assert.NoError(t, err)
		assert.Equal(t, "realm-a", realmName)
		assert.Equal(t, "zonegroup-a", zonegroupName)
		assert.Equal(t, "zone-a", zoneName)
		assert.Equal(t, "realm-a", rootPoolNamespace)

		realm.Spec.IsolatedRootPool = false
		clusterdCtx = &clusterd.Context{RookClientset: rookfake.NewSimpleClientset(zone, zonegroup, realm)}
		_, _, _, rootPoolNamespace, err = getMultisiteForObjectStore(ctx, clusterdCtx, spec, namespace, "my-store")
		assert.NoError(t, err)
		assert.Equal(t, "", rootPoolNamespace)
	})
}
