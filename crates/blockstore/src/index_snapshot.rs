use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::StreamExt as _;
use krabka_units::{ByteSize, convert::ByteSizeExt, mebibytes};
use object_store::{
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, path::Path,
};
use refined_type::rule::GreaterUsize;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use xxhash_rust::xxh3::xxh3_128;

use crate::{
    error::{BlockStoreError, Result},
    index::{IndexShardRange, parse_shard_bound_key, shard_bound_key},
    path_escape::{escape_object_path_segment, unescape_object_path_segment},
};

#[cfg(test)]
mod tests {

    /// `Display` is how the retention reaches config output and log lines.
    /// Writing nothing still succeeds, and reports a retention of "".
    #[test]
    fn retain_displays_its_value() {
        let retain = IndexSnapshotRetain::new(7).expect("7 is a positive retention");
        assert2::check!(retain.to_string() == "7");
        assert2::check!(IndexSnapshotRetain::default().to_string() != "");
    }
    use krabka_units::{convert::ByteSizeExt as _, mebibytes};

    use super::{DEFAULT_INDEX_SNAPSHOT_MAX, IndexSnapshotRetain};

    #[test]
    fn index_snapshot_settings_preserve_defaults_and_validate_input() {
        assert_eq!(DEFAULT_INDEX_SNAPSHOT_MAX.bytes_u64(), 256 * 1024 * 1024);
        assert_eq!(IndexSnapshotRetain::default().into_value(), 8);
        assert_eq!(DEFAULT_INDEX_SNAPSHOT_MAX, mebibytes(256));
        assert_eq!(
            "1".parse::<IndexSnapshotRetain>()
                .expect("one retained snapshot is valid")
                .into_value(),
            1
        );

        for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
            assert!(
                invalid.parse::<IndexSnapshotRetain>().is_err(),
                "{invalid:?} should be rejected"
            );
        }
    }

    mod sharding {
        use std::sync::Arc;

        use assert2::check;
        use object_store::{ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path};

        use crate::{
            IndexShardRange,
            index::shard_bound_key,
            index_snapshot::{
                MAX_SHARD_SLOTS_PER_RECORD, SnapshotManifest, UNBOUNDED_SHARD_RANGE,
                is_shard_payload_location, put_manifest_snapshot, put_shard_payload,
                shard_payload_content_hash, shard_payload_object_key, shard_payload_prefix_for_key,
                shard_ranges_for_span,
            },
            unescape_object_path_segment,
        };

        const LABEL: &str = "test index snapshot";
        const KEY: &str = "index/test.json";

        fn store() -> Arc<dyn ObjectStore> {
            Arc::new(InMemory::new())
        }

        async fn payload_keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
            use futures::StreamExt as _;

            let mut listing = store.list(Some(&Path::from("index/test/payloads")));
            let mut keys = Vec::new();
            while let Some(meta) = listing.next().await {
                keys.push(meta.unwrap().location.to_string());
            }
            keys.sort();
            keys
        }

        /// Publishes one shard of `content` for `tenant` over `range`.
        async fn publish(
            store: &Arc<dyn ObjectStore>,
            tenant: &str,
            range: IndexShardRange,
            content: &[u8],
        ) -> String {
            let hash = shard_payload_content_hash(content);
            put_shard_payload(store, KEY, tenant, range, &hash, content.to_vec())
                .await
                .unwrap();
            let hash_for_merge = hash.clone();
            put_manifest_snapshot(
                store,
                KEY,
                crate::IndexSnapshotRetain::new(1).unwrap(),
                crate::DEFAULT_INDEX_SNAPSHOT_MAX,
                LABEL,
                |_| {
                    let hash = hash_for_merge.clone();
                    async move {
                        let mut manifest = SnapshotManifest::new();
                        manifest.insert(tenant, range, hash);
                        Ok(manifest)
                    }
                },
            )
            .await
            .unwrap();
            shard_payload_object_key(KEY, tenant, range, &hash)
        }

        /// The grid is what makes an append touch one shard rather than the
        /// tenant, so where a record lands has to be a function of its own
        /// span and nothing else.
        #[test]
        fn a_record_belongs_to_every_slot_its_span_crosses() {
            for (name, min_ts, max_ts, expected) in [
                ("inside one slot", 10_i64, 90_i64, vec![(0_i64, 99_i64)]),
                ("exactly one slot", 0, 99, vec![(0, 99)]),
                ("across a boundary", 90, 110, vec![(0, 99), (100, 199)]),
                (
                    "three slots",
                    50,
                    250,
                    vec![(0, 99), (100, 199), (200, 299)],
                ),
                ("before the epoch", -10, -1, vec![(-100, -1)]),
                (
                    "reversed bounds",
                    250,
                    50,
                    vec![(0, 99), (100, 199), (200, 299)],
                ),
            ] {
                let got = shard_ranges_for_span(min_ts, max_ts, 100);

                check!(
                    got == expected
                        .into_iter()
                        .map(|(start, end)| IndexShardRange::new(start, end))
                        .collect::<Vec<_>>(),
                    "{name}"
                );
            }
        }

        /// One block with a broken clock must not become a million objects.
        #[test]
        fn a_record_wider_than_the_grid_allows_leaves_it() {
            let width = 100;
            let just_inside = width * (MAX_SHARD_SLOTS_PER_RECORD - 1);
            let too_wide = width * MAX_SHARD_SLOTS_PER_RECORD;

            check!(
                shard_ranges_for_span(0, just_inside, width).len()
                    == usize::try_from(MAX_SHARD_SLOTS_PER_RECORD).unwrap()
            );
            check!(shard_ranges_for_span(0, too_wide, width) == vec![UNBOUNDED_SHARD_RANGE]);
        }

        /// A payload is named by its own bytes, so two writers that encode the
        /// same shard write the same object and neither can clobber the other.
        #[test]
        fn a_payload_key_is_a_function_of_its_bytes() {
            let range = IndexShardRange::new(0, 99);

            let same = shard_payload_object_key(KEY, "t", range, &shard_payload_content_hash(b"a"));
            let again =
                shard_payload_object_key(KEY, "t", range, &shard_payload_content_hash(b"a"));
            let other =
                shard_payload_object_key(KEY, "t", range, &shard_payload_content_hash(b"b"));

            check!(same == again);
            check!(same != other);
            check!(is_shard_payload_location(KEY, &same));
        }

        /// The sweep is the only thing that reclaims a superseded payload, and
        /// the only thing that could delete a live one. It has to tell them
        /// apart from the manifests alone.
        #[tokio::test]
        async fn the_sweep_reclaims_what_no_retained_manifest_names() {
            let store = store();
            let range = IndexShardRange::new(0, 99);
            let superseded = publish(&store, "t", range, b"first").await;
            let live = publish(&store, "t", range, b"second").await;
            check!(payload_keys(&store).await.len() == 2);

            crate::index_snapshot::sweep_orphan_shard_payloads(
                &store,
                KEY,
                crate::DEFAULT_INDEX_SNAPSHOT_MAX,
                LABEL,
                std::time::Duration::ZERO,
            )
            .await
            .unwrap();

            check!(payload_keys(&store).await == vec![live.clone()]);
            check!(store.head(&Path::from(superseded)).await.is_err());
            check!(store.head(&Path::from(live)).await.is_ok());
        }

        /// A writer puts its payloads and only then swaps the manifest, so
        /// between the two its payloads are referenced by nothing and look
        /// exactly like orphans. Deleting one there would leave the winner
        /// naming an object that is gone.
        #[tokio::test]
        async fn the_sweep_leaves_a_payload_no_manifest_names_yet_alone() {
            let store = store();
            let range = IndexShardRange::new(0, 99);
            publish(&store, "t", range, b"published").await;
            let hash = shard_payload_content_hash(b"in flight");
            put_shard_payload(&store, KEY, "t", range, &hash, b"in flight".to_vec())
                .await
                .unwrap();
            let in_flight = shard_payload_object_key(KEY, "t", range, &hash);

            crate::index_snapshot::sweep_orphan_shard_payloads(
                &store,
                KEY,
                crate::DEFAULT_INDEX_SNAPSHOT_MAX,
                LABEL,
                std::time::Duration::from_hours(1),
            )
            .await
            .unwrap();

            check!(store.head(&Path::from(in_flight)).await.is_ok());
        }

        /// The prefix belongs to the index, but a bucket is shared, and being
        /// wrong about that costs somebody else their object.
        #[tokio::test]
        async fn the_sweep_leaves_objects_it_did_not_write_alone() {
            let store = store();
            let foreign = Path::from("index/test/payloads/tenant=t/not-a-shard.txt");
            store
                .put(&foreign, object_store::PutPayload::from_static(b"mine"))
                .await
                .unwrap();

            crate::index_snapshot::sweep_orphan_shard_payloads(
                &store,
                KEY,
                crate::DEFAULT_INDEX_SNAPSHOT_MAX,
                LABEL,
                std::time::Duration::ZERO,
            )
            .await
            .unwrap();

            check!(store.head(&foreign).await.is_ok());
        }

        /// A payload key names the tenant it belongs to, and the sweep decides
        /// from that key alone whether an object is the index's to delete.
        #[test]
        fn a_tenant_never_widens_a_payload_key_past_its_own_segment() {
            let awkward = [
                ("plain", "tenant-a"),
                ("separator", "a/b"),
                ("relative", ".."),
                ("current", "."),
                ("traversal", "../../etc"),
                ("absolute", "/etc/passwd"),
                ("space", "a b"),
                ("star", "a*b"),
                ("marker", "a!b"),
                ("quote", "a'b"),
                ("brackets", "(a)"),
                ("backslash", "a\\b"),
                ("non ASCII", "\u{e9}"),
            ];
            let prefix = shard_payload_prefix_for_key(KEY);
            let range = IndexShardRange::new(10, 20);
            let content = shard_payload_content_hash(b"payload");

            for (name, tenant) in awkward {
                let key = shard_payload_object_key(KEY, tenant, range, &content);
                let rest = key
                    .strip_prefix(&prefix)
                    .expect("a payload key sits under the payload prefix of its key");
                let segments = rest.trim_start_matches('/').split('/').collect::<Vec<_>>();

                check!(segments.len() == 3, "{name}");
                check!(
                    !segments
                        .iter()
                        .any(|segment| matches!(*segment, "." | ".." | "")),
                    "{name}"
                );
                check!(
                    segments[0]
                        .strip_prefix("tenant=")
                        .and_then(unescape_object_path_segment)
                        == Some(tenant.to_string()),
                    "{name}"
                );
                check!(is_shard_payload_location(KEY, &key), "{name}");
                // The sweep matches a manifest's key against a listed
                // location, so the two have to be the same string.
                check!(Path::from(key.clone()).as_ref() == key.as_str(), "{name}");
            }
        }

        #[test]
        fn a_tenant_segment_the_writer_could_not_have_written_is_foreign() {
            let prefix = shard_payload_prefix_for_key(KEY);
            let span = format!("time={}-{}", shard_bound_key(10), shard_bound_key(20));

            for tenant_segment in ["t", "tenant=a!zz"] {
                let location = format!("{prefix}/{tenant_segment}/{span}/a.kbs");
                check!(
                    !is_shard_payload_location(KEY, &location),
                    "{tenant_segment}"
                );
            }
        }
    }
}

mod default_index_snapshot_max;
mod default_index_snapshot_retain;
mod index_snapshot_prefix_for_key;
mod index_snapshot_retain;
mod latest_index_snapshot_path;
mod list_index_snapshot_objects;
mod manifest_shard;
mod manifest_snapshot_base;
mod max_shard_slots_per_record;
mod parse_shard_payload_location;
mod pending_block_additions;
mod pending_block_removals;
mod pending_removal;
mod prune_old_index_snapshots;
mod put_manifest_snapshot;
mod put_shard_payload;
mod read_index_snapshot_bytes;
mod read_latest_snapshot_manifest;
mod read_manifest_snapshot_base;
mod read_shard_payload;
mod shard_payload_content_hash;
mod shard_payload_object_key;
mod shard_payload_prefix_for_key;
mod shard_payload_sweep_grace;
mod shard_range_of_slot;
mod shard_ranges_for_span;
mod snapshot_generation_from_path;
mod snapshot_key_for_generation;
mod snapshot_manifest;
mod snapshot_manifest_version;
mod snapshot_sweep_interval;
mod sweep_orphan_shard_payloads;
mod unbounded_shard_range;

pub use default_index_snapshot_max::DEFAULT_INDEX_SNAPSHOT_MAX;
pub use default_index_snapshot_retain::DEFAULT_INDEX_SNAPSHOT_RETAIN;
pub use index_snapshot_prefix_for_key::index_snapshot_prefix_for_key;
pub use index_snapshot_retain::IndexSnapshotRetain;
pub use latest_index_snapshot_path::latest_index_snapshot_path;
pub use list_index_snapshot_objects::list_index_snapshot_objects;
pub(crate) use manifest_shard::ManifestShard;
pub(crate) use manifest_snapshot_base::ManifestSnapshotBase;
pub(crate) use max_shard_slots_per_record::MAX_SHARD_SLOTS_PER_RECORD;
pub(crate) use parse_shard_payload_location::is_shard_payload_location;
pub(crate) use pending_block_additions::PendingBlockAdditions;
pub(crate) use pending_block_removals::PendingBlockRemovals;
pub(crate) use pending_removal::PendingRemoval;
use prune_old_index_snapshots::prune_old_index_snapshots;
pub(crate) use put_manifest_snapshot::put_manifest_snapshot;
pub(crate) use put_shard_payload::put_shard_payload;
pub(crate) use read_index_snapshot_bytes::{IndexSnapshotBytes, read_index_snapshot_bytes};
pub(crate) use read_latest_snapshot_manifest::read_latest_snapshot_manifest;
pub(crate) use read_manifest_snapshot_base::read_manifest_snapshot_base;
pub(crate) use read_shard_payload::read_shard_payload;
pub(crate) use shard_payload_content_hash::shard_payload_content_hash;
pub(crate) use shard_payload_object_key::shard_payload_object_key;
pub(crate) use shard_payload_prefix_for_key::shard_payload_prefix_for_key;
pub(crate) use shard_payload_sweep_grace::SHARD_PAYLOAD_SWEEP_GRACE;
pub(crate) use shard_range_of_slot::shard_range_of_slot;
pub(crate) use shard_ranges_for_span::shard_ranges_for_span;
use snapshot_generation_from_path::snapshot_generation_from_path;
use snapshot_key_for_generation::snapshot_key_for_generation;
pub(crate) use snapshot_manifest::SnapshotManifest;
pub(crate) use snapshot_manifest_version::SNAPSHOT_MANIFEST_VERSION;
pub(crate) use snapshot_sweep_interval::SNAPSHOT_SWEEP_INTERVAL;
pub(crate) use sweep_orphan_shard_payloads::sweep_orphan_shard_payloads;
pub(crate) use unbounded_shard_range::UNBOUNDED_SHARD_RANGE;
