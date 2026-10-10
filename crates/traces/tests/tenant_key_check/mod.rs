// The tenant-segment checks the block-building and compaction suites run on
// the object keys they mint.

use assert2::check;
use krabka_blockstore::unescape_object_path_segment;
use object_store::path::Path;

/// One tenant-escaping case: what the case is called, and the tenant it keys.
#[derive(Clone, Copy)]
pub struct TenantKeyCase<'a> {
    pub name: &'a str,
    pub tenant: &'a str,
}

/// Checks that the object store keeps `key` as written, and that its second
/// segment unescapes back to the case's tenant.
pub fn check_tenant_key_round_trips(key: &str, case: TenantKeyCase<'_>) {
    let TenantKeyCase { name, tenant } = case;
    let path = Path::from(key);
    check!(
        path.as_ref() == key,
        "{name}: the store keeps the key as written"
    );
    let parts: Vec<_> = path.parts().collect();
    check!(parts.len() == 4, "{name}");
    check!(
        unescape_object_path_segment(parts[1].as_ref()) == Some(tenant.to_string()),
        "{name}"
    );
}
