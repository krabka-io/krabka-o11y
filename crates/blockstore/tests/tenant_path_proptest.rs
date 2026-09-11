//! Round-trip properties of a tenant name on its way into an object key.

use krabka_blockstore::{
    BlockKey, TenantId, TimeRange, block_path, escape_object_path_segment, index_shard_object_key,
    index_shard_tenant_prefix, index_shards_prefix_for_key, index_unbound_series_object_key,
    unescape_object_path_segment,
};
use object_store::path::Path as ObjectPath;
use proptest::prelude::*;

const KEY: &str = "index/metrics.json";

fn valid_tenant_id() -> impl Strategy<Value = TenantId> {
    "[A-Za-z0-9!_.*'()-]{1,60}".prop_filter_map("`.` and `..` are not tenant ids", |raw| {
        TenantId::new(raw).ok()
    })
}

proptest! {
    #[test]
    fn an_escaped_tenant_reads_back_as_itself(tenant in valid_tenant_id()) {
        let escaped = escape_object_path_segment(tenant.as_str());
        let read_back = unescape_object_path_segment(&escaped);
        prop_assert_eq!(read_back.as_deref(), Some(tenant.as_str()));
    }

    #[test]
    fn an_escaped_tenant_is_one_ordinary_path_segment(tenant in valid_tenant_id()) {
        let escaped = escape_object_path_segment(tenant.as_str());

        prop_assert!(!escaped.is_empty());
        prop_assert!(!escaped.contains('/'));
        prop_assert!(!escaped.contains('\\'));
        prop_assert_ne!(escaped.as_str(), ".");
        prop_assert_ne!(escaped.as_str(), "..");
        // `object_store` escapes a segment again on the way into a `Path`, and
        // an escape that did not survive that would give a key and a listed
        // location two spellings.
        let stored = ObjectPath::from(escaped.clone());
        prop_assert_eq!(stored.as_ref(), escaped.as_str());
    }

    #[test]
    fn a_tenant_adds_exactly_one_segment_to_an_index_key(tenant in valid_tenant_id()) {
        let shards_prefix = index_shards_prefix_for_key(KEY);
        let keys = [
            index_shard_tenant_prefix(KEY, tenant.as_str()),
            index_shard_object_key(KEY, tenant.as_str(), krabka_blockstore::IndexShardRange::new(10, 20)),
            index_unbound_series_object_key(KEY, tenant.as_str()),
        ];

        for (key, extra) in keys.iter().zip([1_usize, 3, 2]) {
            let segments = key.split('/').count();
            prop_assert_eq!(segments, shards_prefix.split('/').count() + extra);
            prop_assert!(!key.split('/').any(|segment| matches!(segment, "." | ".." | "")));
            let stored = ObjectPath::from(key.clone());
            prop_assert_eq!(stored.as_ref(), key.as_str());
        }
    }

    #[test]
    fn a_block_key_names_four_segments_and_keeps_its_root(tenant in valid_tenant_id()) {
        let key = BlockKey::new(
            tenant.as_str(),
            0,
            0,
            1,
            TimeRange::new(0, 1).expect("0 to 1 is a time range"),
        );
        let object_key = key.object_key();

        prop_assert_eq!(object_key.split('/').count(), 4);
        prop_assert!(
            !object_key
                .split('/')
                .any(|segment| matches!(segment, "." | ".." | ""))
        );

        let root = std::path::Path::new("/data/store");
        let path = block_path(root, &key);
        prop_assert!(path.starts_with(root));
        prop_assert!(
            !path
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        );
    }
}
