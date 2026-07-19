---
title: CephObjectRealm CRD
---

Rook allows creation of a realm in a [Ceph Object Multisite](../../Storage-Configuration/Object-Storage-RGW/ceph-object-multisite.md)
configuration through a CRD. The following settings are available for Ceph object store realms.

## Example

```yaml
apiVersion: ceph.rook.io/v1
kind: CephObjectRealm
metadata:
  name: realm-a
  namespace: rook-ceph
# This endpoint in this section needs is an endpoint from the master zone  in the master zone group of realm-a. See object-multisite.md for more details.
spec:
  pull:
    endpoint: http://10.2.105.133:80
  defaultRealm: true
```

## Settings

### Metadata

* `name`: The name of the object realm to create
* `namespace`: The namespace of the Rook cluster where the object realm is created.

### Spec

* `pull`: This optional section is for the pulling the realm for another ceph cluster.
    * `endpoint`: The endpoint in the realm from another ceph cluster you want to pull from. This endpoint must be in the master zone of the master zone group of the realm.
* `defaultRealm`: When set to true, Rook will mark the CephObjectStore's realm as the default realm in the Ceph cluster. Only one realm can be marked default. Ceph does not allow default to be unassigned after it is assigned; a different realm can be marked default instead.
* `isolatedRootPool`: When set to true, the realm's RGW topology records (realm, zone group, zone, and period)
    are stored in a RADOS namespace of the `.rgw.root` pool named after the realm, instead of sharing the
    un-namespaced `.rgw.root` with every other realm in the Ceph cluster. The zone group, zone, and object store
    controllers for this realm follow this setting automatically. The setting is immutable and only applies to
    newly created (or pulled) realms: existing RGW topology cannot be relocated, and Rook refuses to reconcile
    a realm whose records already exist at the other `.rgw.root` location (e.g. after deleting and re-creating
    the CR with a different `isolatedRootPool` value).
    See [Isolating the RGW topology pool](../../Storage-Configuration/Object-Storage-RGW/object-storage.md#isolating-the-rgw-topology-pool)
    for details and operational notes.
