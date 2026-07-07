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
	"encoding/json"
	"strings"
	"testing"
	"time"

	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	"github.com/rook/rook/pkg/clusterd"
	"github.com/rook/rook/pkg/daemon/ceph/client"
	clienttest "github.com/rook/rook/pkg/daemon/ceph/client/test"
	"github.com/rook/rook/pkg/operator/ceph/config"
	"github.com/rook/rook/pkg/operator/test"
	exectest "github.com/rook/rook/pkg/util/exec/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// sharedZoneJSON is a shared-pool zone config: every pool field is namespaced into the two
// shared pools, plus a custom storage class on a third pool.
const sharedZoneJSON = `{
	"id": "z1",
	"name": "tenant-a",
	"domain_root": "rgw-meta:tenant-a.meta.root",
	"control_pool": "rgw-meta:tenant-a.control",
	"gc_pool": "rgw-meta:tenant-a.log.gc",
	"log_pool": "rgw-meta:tenant-a.log",
	"user_uid_pool": "rgw-meta:tenant-a.meta.users.uid",
	"otp_pool": "rgw-meta:tenant-a.otp",
	"oidc_pool": "rgw-meta:tenant-a.meta.oidc",
	"notif_pool": "rgw-meta:tenant-a.log.notif",
	"placement_pools": [
		{
			"key": "default-placement",
			"val": {
				"index_pool": "rgw-meta:tenant-a.buckets.index",
				"storage_classes": {
					"STANDARD": {"data_pool": "rgw-data:tenant-a.buckets.data"},
					"GLACIER": {"data_pool": "slow-data:tenant-a.GLACIER"}
				},
				"data_extra_pool": "rgw-meta:tenant-a.buckets.non-ec",
				"inline_data": true
			}
		}
	],
	"realm_id": "8e29e15c-8917-4e42-b199-a79c8e4d3d97"
}`

const sharedZoneExpectedOSDCap = `allow rwx pool=.rgw.root, ` +
	`allow rwx pool=rgw-data namespace=tenant-a.buckets.data, ` +
	`allow rwx pool=rgw-meta namespace=tenant-a.buckets.index, ` +
	`allow rwx pool=rgw-meta namespace=tenant-a.buckets.non-ec, ` +
	`allow rwx pool=rgw-meta namespace=tenant-a.control, ` +
	`allow rwx pool=rgw-meta namespace=tenant-a.log, ` +
	`allow rwx pool=rgw-meta namespace=tenant-a.log.gc, ` +
	`allow rwx pool=rgw-meta namespace=tenant-a.log.notif, ` +
	`allow rwx pool=rgw-meta namespace=tenant-a.meta.oidc, ` +
	`allow rwx pool=rgw-meta namespace=tenant-a.meta.root, ` +
	`allow rwx pool=rgw-meta namespace=tenant-a.meta.users.uid, ` +
	`allow rwx pool=rgw-meta namespace=tenant-a.otp, ` +
	`allow rwx pool=slow-data namespace=tenant-a.GLACIER`

// dedicatedZoneJSON is a dedicated-pool zone config: bare per-store pools, several roles living
// in namespaces inside them (Ceph's defaults), and a standalone dedup pool (Ceph v20 default).
const dedicatedZoneJSON = `{
	"id": "z2",
	"name": "my-store",
	"domain_root": "my-store.rgw.meta:root",
	"control_pool": "my-store.rgw.control",
	"gc_pool": "my-store.rgw.log:gc",
	"lc_pool": "my-store.rgw.log:lc",
	"log_pool": "my-store.rgw.log",
	"user_uid_pool": "my-store.rgw.meta:users.uid",
	"otp_pool": "my-store.rgw.otp",
	"dedup_pool": "my-store.rgw.dedup",
	"placement_pools": [
		{
			"key": "default-placement",
			"val": {
				"index_pool": "my-store.rgw.buckets.index",
				"storage_classes": {
					"STANDARD": {"data_pool": "my-store.rgw.buckets.data"}
				},
				"data_extra_pool": "my-store.rgw.buckets.non-ec"
			}
		}
	]
}`

const dedicatedZoneExpectedOSDCap = `allow rwx pool=.rgw.root, ` +
	`allow rwx pool=my-store.rgw.buckets.data, ` +
	`allow rwx pool=my-store.rgw.buckets.index, ` +
	`allow rwx pool=my-store.rgw.buckets.non-ec, ` +
	`allow rwx pool=my-store.rgw.control, ` +
	`allow rwx pool=my-store.rgw.dedup, ` +
	`allow rwx pool=my-store.rgw.log, ` +
	`allow rwx pool=my-store.rgw.log namespace=gc, ` +
	`allow rwx pool=my-store.rgw.log namespace=lc, ` +
	`allow rwx pool=my-store.rgw.meta namespace=root, ` +
	`allow rwx pool=my-store.rgw.meta namespace=users.uid, ` +
	`allow rwx pool=my-store.rgw.otp`

func unmarshalZone(t *testing.T, zoneJSON string) map[string]interface{} {
	t.Helper()
	zone := map[string]interface{}{}
	require.NoError(t, json.Unmarshal([]byte(zoneJSON), &zone))
	return zone
}

func Test_zonePoolGrants(t *testing.T) {
	t.Run("collects every pool field including nested placement pools", func(t *testing.T) {
		grants := zonePoolGrants(unmarshalZone(t, sharedZoneJSON))
		assert.Contains(t, grants, poolNamespace{Pool: "rgw-meta", Namespace: "tenant-a.meta.root"})      // domain_root
		assert.Contains(t, grants, poolNamespace{Pool: "rgw-meta", Namespace: "tenant-a.buckets.index"})  // placement index_pool
		assert.Contains(t, grants, poolNamespace{Pool: "rgw-meta", Namespace: "tenant-a.buckets.non-ec"}) // placement data_extra_pool
		assert.Contains(t, grants, poolNamespace{Pool: "rgw-data", Namespace: "tenant-a.buckets.data"})   // storage class STANDARD
		assert.Contains(t, grants, poolNamespace{Pool: "slow-data", Namespace: "tenant-a.GLACIER"})       // custom storage class
		assert.Contains(t, grants, poolNamespace{Pool: "rgw-meta", Namespace: "tenant-a.meta.oidc"})      // role unknown to Rook's shared-pool map
		assert.Len(t, grants, 12)
	})
	t.Run("bare pool values yield whole-pool grants", func(t *testing.T) {
		grants := zonePoolGrants(unmarshalZone(t, dedicatedZoneJSON))
		assert.Contains(t, grants, poolNamespace{Pool: "my-store.rgw.control"})
		assert.Contains(t, grants, poolNamespace{Pool: "my-store.rgw.log", Namespace: "gc"})
		assert.Contains(t, grants, poolNamespace{Pool: "my-store.rgw.dedup"})
	})
	t.Run("ignores non-pool fields and empty values", func(t *testing.T) {
		grants := zonePoolGrants(map[string]interface{}{
			"id":        "x",
			"name":      "n",
			"realm_id":  "r",
			"log_pool":  "",
			"unrelated": map[string]interface{}{"list": []interface{}{"a"}},
		})
		assert.Empty(t, grants)
	})
}

func Test_buildOSDCapString(t *testing.T) {
	tests := []struct {
		name    string
		grants  []poolNamespace
		want    string
		wantErr bool
	}{
		{
			name:   "whole pool and namespaced grants, sorted and de-duplicated",
			grants: []poolNamespace{{Pool: "b", Namespace: "ns"}, {Pool: "a"}, {Pool: "b", Namespace: "ns"}},
			want:   "allow rwx pool=a, allow rwx pool=b namespace=ns",
		},
		{
			name:   "empty pool entries are skipped",
			grants: []poolNamespace{{Pool: ""}, {Pool: "a"}},
			want:   "allow rwx pool=a",
		},
		{
			name:    "no grants is an error",
			grants:  []poolNamespace{},
			wantErr: true,
		},
		{
			name:    "wildcard in pool is rejected",
			grants:  []poolNamespace{{Pool: "a*"}},
			wantErr: true,
		},
		{
			name:    "space in namespace is rejected",
			grants:  []poolNamespace{{Pool: "a", Namespace: "n s"}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildOSDCapString(tt.grants)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_scopedRGWDaemonOSDCap(t *testing.T) {
	t.Run("shared-pool zone", func(t *testing.T) {
		got, err := scopedRGWDaemonOSDCap(unmarshalZone(t, sharedZoneJSON))
		assert.NoError(t, err)
		assert.Equal(t, sharedZoneExpectedOSDCap, got)
		assert.NotContains(t, got, "*")
	})
	t.Run("dedicated-pool zone", func(t *testing.T) {
		got, err := scopedRGWDaemonOSDCap(unmarshalZone(t, dedicatedZoneJSON))
		assert.NoError(t, err)
		assert.Equal(t, dedicatedZoneExpectedOSDCap, got)
		assert.NotContains(t, got, "*")
	})
	t.Run("byte-stable across invocations", func(t *testing.T) {
		// the mon compares caps byte-wise on `auth get-or-create-key`; any drift would re-cap
		// the daemon and restart the pod on every reconcile
		first, err := scopedRGWDaemonOSDCap(unmarshalZone(t, sharedZoneJSON))
		assert.NoError(t, err)
		second, err := scopedRGWDaemonOSDCap(unmarshalZone(t, sharedZoneJSON))
		assert.NoError(t, err)
		assert.Equal(t, first, second)
	})
	t.Run("zone without pools is an error", func(t *testing.T) {
		_, err := scopedRGWDaemonOSDCap(map[string]interface{}{"id": "x"})
		assert.Error(t, err)
	})
}

func Test_mayAffectPoolLayout(t *testing.T) {
	affecting := []string{
		"rgw_realm_root_pool",
		"rgw zone root pool",   // Ceph accepts space-separated option names
		"rgw-period-root-pool", // and dash-separated
		"rgw_zonegroup",
		"rgw_realm",
		"rgw_region",
		"rgw_otp_pool",
	}
	for _, key := range affecting {
		assert.True(t, mayAffectPoolLayout(key), key)
	}
	benign := []string{
		"rgw_enable_usage_log",
		"rgw_thread_pool_size",
		"debug_rgw",
		"rgw_max_chunk_size",
	}
	for _, key := range benign {
		assert.False(t, mayAffectPoolLayout(key), key)
	}
}

func Test_poolAffectingConfigReason(t *testing.T) {
	tests := []struct {
		name        string
		gateway     cephv1.GatewaySpec
		clusterSpec *cephv1.ClusterSpec
		wantReason  bool
	}{
		{
			name:       "clean specs",
			gateway:    cephv1.GatewaySpec{RgwConfig: map[string]string{"rgw_enable_usage_log": "true"}},
			wantReason: false,
		},
		{
			name:       "rgwConfig pool redirect",
			gateway:    cephv1.GatewaySpec{RgwConfig: map[string]string{"rgw_zone_root_pool": "elsewhere"}},
			wantReason: true,
		},
		{
			name:       "rgwConfigFromSecret pool redirect detected by key name only",
			gateway:    cephv1.GatewaySpec{RgwConfigFromSecret: map[string]v1.SecretKeySelector{"rgw_realm_root_pool": {}}},
			wantReason: true,
		},
		{
			name:       "rgwCommandFlags pool redirect",
			gateway:    cephv1.GatewaySpec{RgwCommandFlags: map[string]string{"rgw-zone-root-pool": "elsewhere"}},
			wantReason: true,
		},
		{
			name:        "cluster cephConfig global pool redirect",
			clusterSpec: &cephv1.ClusterSpec{CephConfig: map[string]map[string]string{"global": {"rgw_period_root_pool": "elsewhere"}}},
			wantReason:  true,
		},
		{
			name:        "cluster cephConfig client.rgw section pool redirect",
			clusterSpec: &cephv1.ClusterSpec{CephConfig: map[string]map[string]string{"client.rgw.my.store.a": {"rgw_realm": "other"}}},
			wantReason:  true,
		},
		{
			name:        "cluster cephConfig non-rgw section is ignored",
			clusterSpec: &cephv1.ClusterSpec{CephConfig: map[string]map[string]string{"osd": {"osd_memory_target_pool": "x"}}},
			wantReason:  false,
		},
		{
			name:        "cluster cephConfigFromSecret pool redirect",
			clusterSpec: &cephv1.ClusterSpec{CephConfigFromSecret: map[string]map[string]v1.SecretKeySelector{"client": {"rgw_zone_root_pool": {}}}},
			wantReason:  true,
		},
		{
			name:       "nil cluster spec",
			gateway:    cephv1.GatewaySpec{},
			wantReason: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason := poolAffectingConfigReason(&tt.gateway, tt.clusterSpec)
			if tt.wantReason {
				assert.NotEmpty(t, reason)
			} else {
				assert.Empty(t, reason)
			}
		})
	}
}

// Test_startRGWPods_cephxLeastPrivilege exercises the full wiring: zone config fetched via
// radosgw-admin, grants enumerated, and the scoped (or broad) caps applied to the cephx user and
// written into the keyring secret.
func Test_startRGWPods_cephxLeastPrivilege(t *testing.T) {
	ctx := context.TODO()

	newTest := func(zoneJSON string) (*clusterConfig, *[]string) {
		clientset := test.New(t, 3)
		var capturedAuthArgs []string
		executor := &exectest.MockExecutor{
			MockExecuteCommandWithOutput: func(command string, args ...string) (string, error) {
				if args[0] == "auth" && args[1] == "get-or-create-key" {
					capturedAuthArgs = append([]string{}, args...)
					return `{"key":"mysecurekey"}`, nil
				}
				return `{"id":"test-id"}`, nil
			},
			MockExecuteCommandWithTimeout: func(timeout time.Duration, command string, args ...string) (string, error) {
				if command == "radosgw-admin" && args[0] == "zone" && args[1] == "get" {
					return zoneJSON, nil
				}
				return "", nil
			},
		}
		info := clienttest.CreateTestClusterInfo(1)
		context := &clusterd.Context{Clientset: clientset, Executor: executor, ConfigDir: t.TempDir()}
		store := simpleStore()
		store.Spec.Gateway.CephxLeastPrivilege = true
		data := config.NewStatelessDaemonDataPathMap(config.RgwType, "my-fs", "rook-ceph", "/var/lib/rook/")
		ownerInfo := client.NewMinimumOwnerInfoWithOwnerRef()
		c := &clusterConfig{
			context:     context,
			clusterInfo: info,
			store:       store,
			clusterSpec: &cephv1.ClusterSpec{},
			ownerInfo:   ownerInfo,
			DataPathMap: data,
		}
		return c, &capturedAuthArgs
	}

	keyringCaps := func(t *testing.T, c *clusterConfig) string {
		t.Helper()
		secretName := instanceName(c.store.Name) + "-a-keyring"
		secret, err := c.context.Clientset.CoreV1().Secrets(c.clusterInfo.Namespace).Get(ctx, secretName, metav1.GetOptions{})
		require.NoError(t, err)
		// the fake clientset does not convert StringData to Data
		return secret.StringData["keyring"]
	}

	t.Run("scoped caps from zone config", func(t *testing.T) {
		c, authArgs := newTest(dedicatedZoneJSON)
		err := c.startRGWPods(c.store.Name, c.store.Name, c.store.Name, nil)
		require.NoError(t, err)
		assert.Equal(t, "scoped", c.cephxCapsStatus)
		keyring := keyringCaps(t, c)
		assert.Contains(t, keyring, `caps osd = "`+dedicatedZoneExpectedOSDCap+`"`)
		assert.Contains(t, keyring, `caps mon = "allow rw"`)
		assert.Contains(t, strings.Join(*authArgs, " "), dedicatedZoneExpectedOSDCap)
	})

	t.Run("pool-affecting rgwConfig keeps broad caps", func(t *testing.T) {
		c, authArgs := newTest(dedicatedZoneJSON)
		c.store.Spec.Gateway.RgwConfig = map[string]string{"rgw_zone_root_pool": "elsewhere"}
		err := c.startRGWPods(c.store.Name, c.store.Name, c.store.Name, nil)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(c.cephxCapsStatus, "broad: "), c.cephxCapsStatus)
		assert.Contains(t, keyringCaps(t, c), `caps osd = "allow rwx"`)
		assert.Contains(t, strings.Join(*authArgs, " "), "osd allow rwx mon allow rw")
	})

	t.Run("feature off keeps broad caps and empty status", func(t *testing.T) {
		c, authArgs := newTest(dedicatedZoneJSON)
		c.store.Spec.Gateway.CephxLeastPrivilege = false
		err := c.startRGWPods(c.store.Name, c.store.Name, c.store.Name, nil)
		require.NoError(t, err)
		assert.Empty(t, c.cephxCapsStatus)
		assert.Contains(t, keyringCaps(t, c), `caps osd = "allow rwx"`)
		assert.Contains(t, strings.Join(*authArgs, " "), "osd allow rwx mon allow rw")
	})

	t.Run("zone get failure fails the reconcile instead of widening caps", func(t *testing.T) {
		c, _ := newTest(`not json`)
		err := c.startRGWPods(c.store.Name, c.store.Name, c.store.Name, nil)
		assert.Error(t, err)
	})
}
