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
	"sort"
	"strings"

	cephv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
)

// poolNamespace is a (RADOS pool, RADOS namespace) grant for the RGW daemon's OSD caps. An empty
// Namespace grants the whole pool, which cephx interprets as matching every namespace in it —
// required for dedicated-pool stores, whose zone config references bare pools whose objects live
// in namespaces (e.g. <zone>.rgw.log:gc).
type poolNamespace struct {
	Pool      string
	Namespace string
}

// zonePoolGrants enumerates every pool[:namespace] referenced by a zone's configuration JSON
// (`radosgw-admin zone get`). The zone config is what the daemon itself reads to decide where all
// I/O goes, so the resulting grants match the daemon's real pool set by construction — including
// pools kept by data-preserving migrations, custom pool placements, and pool roles introduced by
// newer Ceph versions. It collects every string value whose key ends in "_pool" plus
// "domain_root", recursively (placement_pools entries hold index_pool/data_extra_pool and
// per-storage-class data_pool at nested levels).
func zonePoolGrants(zoneJSON map[string]interface{}) []poolNamespace {
	grants := []poolNamespace{}
	var walk func(v interface{})
	walk = func(v interface{}) {
		switch obj := v.(type) {
		case map[string]interface{}:
			for k, val := range obj {
				if s, ok := val.(string); ok {
					if strings.HasSuffix(k, "_pool") || k == "domain_root" {
						pool, ns, _ := strings.Cut(s, ":")
						if pool != "" {
							grants = append(grants, poolNamespace{Pool: pool, Namespace: ns})
						}
					}
					continue
				}
				walk(val)
			}
		case []interface{}:
			for _, e := range obj {
				walk(e)
			}
		}
	}
	walk(zoneJSON)
	return grants
}

// buildOSDCapString turns pool grants into a single OSD cap string of explicitly-enumerated,
// wildcard-free clauses: "allow rwx pool=P" (whole pool) or "allow rwx pool=P namespace=N".
// The result is sorted, de-duplicated, and joined with a fixed literal ", ": the mon compares
// requested caps against stored caps byte-wise (no normalization), so the string must be
// byte-identical across reconciles for `auth get-or-create-key` to be a no-op.
func buildOSDCapString(grants []poolNamespace) (string, error) {
	seen := map[string]struct{}{}
	caps := make([]string, 0, len(grants))
	for _, g := range grants {
		if g.Pool == "" {
			continue
		}
		if strings.ContainsAny(g.Pool, "* ") || strings.ContainsAny(g.Namespace, "* ") {
			return "", fmt.Errorf("refusing to build osd cap for pool %q namespace %q: invalid character", g.Pool, g.Namespace)
		}
		c := "allow rwx pool=" + g.Pool
		if g.Namespace != "" {
			c += " namespace=" + g.Namespace
		}
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		caps = append(caps, c)
	}
	if len(caps) == 0 {
		return "", fmt.Errorf("no osd cap clauses could be enumerated")
	}
	sort.Strings(caps)
	return strings.Join(caps, ", "), nil
}

// scopedRGWDaemonOSDCap builds the least-privilege OSD cap string for an RGW daemon from its
// zone configuration, plus the whole .rgw.root pool that every RGW reads its
// realm/zonegroup/zone/period records from.
func scopedRGWDaemonOSDCap(zoneJSON map[string]interface{}) (string, error) {
	grants := zonePoolGrants(zoneJSON)
	if len(grants) == 0 {
		return "", fmt.Errorf("zone config references no pools")
	}
	grants = append(grants, poolNamespace{Pool: rootPool})
	return buildOSDCapString(grants)
}

// mayAffectPoolLayout reports whether a Ceph config option key can redirect where RGW reads or
// writes RADOS data outside the zone configuration (e.g. rgw_realm_root_pool). Keys are
// normalized first because Ceph accepts "rgw zone", "rgw-zone", and "rgw_zone" interchangeably.
func mayAffectPoolLayout(key string) bool {
	k := strings.ToLower(strings.NewReplacer(" ", "_", "-", "_").Replace(key))
	return strings.HasSuffix(k, "_pool") ||
		strings.HasPrefix(k, "rgw_zone") ||
		strings.HasPrefix(k, "rgw_realm") ||
		strings.HasPrefix(k, "rgw_region")
}

// poolAffectingConfigReason scans the config surfaces that bypass the zone configuration for
// pool-affecting option keys. Scoped caps derived from the zone config would not cover such
// redirects, so the caller must keep broad caps (fail closed) when a reason is returned. It
// inspects only option key names — never secret values — so no secret fetch is needed.
func poolAffectingConfigReason(gateway *cephv1.GatewaySpec, clusterSpec *cephv1.ClusterSpec) string {
	for key := range gateway.RgwConfig {
		if mayAffectPoolLayout(key) {
			return fmt.Sprintf("spec.gateway.rgwConfig key %q may redirect rgw pools", key)
		}
	}
	for key := range gateway.RgwConfigFromSecret {
		if mayAffectPoolLayout(key) {
			return fmt.Sprintf("spec.gateway.rgwConfigFromSecret key %q may redirect rgw pools", key)
		}
	}
	for key := range gateway.RgwCommandFlags {
		if mayAffectPoolLayout(key) {
			return fmt.Sprintf("spec.gateway.rgwCommandFlags key %q may redirect rgw pools", key)
		}
	}
	if clusterSpec == nil {
		return ""
	}
	rgwConfigSection := func(section string) bool {
		return section == "global" || section == "client" || strings.HasPrefix(section, "client.rgw")
	}
	for section, opts := range clusterSpec.CephConfig {
		if !rgwConfigSection(section) {
			continue
		}
		for key := range opts {
			if mayAffectPoolLayout(key) {
				return fmt.Sprintf("CephCluster spec.cephConfig[%s] key %q may redirect rgw pools", section, key)
			}
		}
	}
	for section, opts := range clusterSpec.CephConfigFromSecret {
		if !rgwConfigSection(section) {
			continue
		}
		for key := range opts {
			if mayAffectPoolLayout(key) {
				return fmt.Sprintf("CephCluster spec.cephConfigFromSecret[%s] key %q may redirect rgw pools", section, key)
			}
		}
	}
	return ""
}
