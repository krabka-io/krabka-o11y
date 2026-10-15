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
use krabka_o11y_verified::{bounded_shard_slots, shard_range};
use krabka_units::{ByteSize, convert::ByteSizeExt, mebibytes};
use object_store::{
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
    path::Path,
};
use refined_type::rule::GreaterUsize;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use xxhash_rust::xxh3::xxh3_128;

use crate::{
    error::{BlockStoreError, Result},
    index::{
        CappedObject, IndexShardObject, IndexShardRange, capped_read_error,
        index_shards_prefix_for_key, oversized_object_error, parse_index_shard_location,
        parse_shard_bound_key, shard_bound_key,
    },
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
            awkward_tenants::AWKWARD_TENANTS,
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

        struct ShardRangeCase {
            name: &'static str,
            min_ts: i64,
            max_ts: i64,
            expected: Vec<IndexShardRange>,
        }

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
            for case in [
                ShardRangeCase {
                    name: "inside one slot",
                    min_ts: 10_i64,
                    max_ts: 90_i64,
                    expected: vec![IndexShardRange::new(0_i64, 99_i64)],
                },
                ShardRangeCase {
                    name: "exactly one slot",
                    min_ts: 0,
                    max_ts: 99,
                    expected: vec![IndexShardRange::new(0, 99)],
                },
                ShardRangeCase {
                    name: "across a boundary",
                    min_ts: 90,
                    max_ts: 110,
                    expected: vec![IndexShardRange::new(0, 99), IndexShardRange::new(100, 199)],
                },
                ShardRangeCase {
                    name: "three slots",
                    min_ts: 50,
                    max_ts: 250,
                    expected: vec![
                        IndexShardRange::new(0, 99),
                        IndexShardRange::new(100, 199),
                        IndexShardRange::new(200, 299),
                    ],
                },
                ShardRangeCase {
                    name: "before the epoch",
                    min_ts: -10,
                    max_ts: -1,
                    expected: vec![IndexShardRange::new(-100, -1)],
                },
                ShardRangeCase {
                    name: "reversed bounds",
                    min_ts: 250,
                    max_ts: 50,
                    expected: vec![
                        IndexShardRange::new(0, 99),
                        IndexShardRange::new(100, 199),
                        IndexShardRange::new(200, 299),
                    ],
                },
            ] {
                let got = shard_ranges_for_span(case.min_ts, case.max_ts, 100);

                check!(got == case.expected, "{}", case.name);
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

            check!(payload_keys(&store).await.len() == 2);
            check!(store.head(&Path::from(superseded)).await.unwrap().size == 0);
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

        #[tokio::test(start_paused = true)]
        async fn a_publication_attempt_expires_before_its_payload_can_age() {
            let error = put_manifest_snapshot(
                &store(),
                KEY,
                crate::IndexSnapshotRetain::new(1).unwrap(),
                crate::DEFAULT_INDEX_SNAPSHOT_MAX,
                LABEL,
                |_| std::future::pending::<crate::Result<SnapshotManifest>>(),
            )
            .await
            .unwrap_err();

            check!(error.to_string().contains("publication exceeded"));
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
            let prefix = shard_payload_prefix_for_key(KEY);
            let range = IndexShardRange::new(10, 20);
            let content = shard_payload_content_hash(b"payload");

            for case in AWKWARD_TENANTS {
                let name = case.name;
                let key = shard_payload_object_key(KEY, case.tenant, range, &content);
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
                        == Some(case.tenant.to_string()),
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
mod index_snapshot_retain;
mod list_index_object_keys;
mod manifest_shard;
mod pending_block_additions;
mod pending_block_removals;
mod snapshot_manifest;
mod snapshot_persistence_methods;
mod snapshot_persistence_requests;
mod sweep_orphan_shard_payloads;
mod tenant_shard_merge;

pub use index_snapshot_retain::IndexSnapshotRetain;
pub use list_index_object_keys::list_index_object_keys;
pub(crate) use manifest_shard::ManifestShard;
pub(crate) use pending_block_additions::PendingBlockAdditions;
pub(crate) use pending_block_removals::{PendingBlockRemoval, PendingBlockRemovals};
pub use snapshot_persistence_requests::{IndexSnapshotPublish, TenantSnapshotRangeRead};

/// Counts every object under the snapshot prefix of `key`, whatever its kind.
#[cfg(test)]
pub(crate) async fn count_snapshot_prefix_objects(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> usize {
    let prefix = Path::from(index_snapshot_prefix_for_key(key));
    let mut stream = store.list(Some(&prefix));
    let mut count = 0;
    while let Some(meta) = stream.next().await {
        meta.unwrap();
        count += 1;
    }
    count
}
pub(crate) use snapshot_manifest::SnapshotManifest;
pub(crate) use snapshot_persistence_methods::snapshot_persistence_methods;
pub(crate) use sweep_orphan_shard_payloads::sweep_orphan_shard_payloads;
pub(crate) use tenant_shard_merge::TenantShardMerge;

/// The state a snapshot write starts from: the manifest the newest generation
/// published, and the generation the write should claim next.
pub(crate) struct ManifestSnapshotBase {
    /// Newest stored manifest, or `None` when the key has no generation yet.
    pub(crate) manifest: Option<SnapshotManifest>,
    /// Generation to write, one past the newest stored one.
    pub(crate) next_generation: u64,
}

/// Most grid slots one record may span before it is widened into the unbounded shard.
///
/// A pathological time span would otherwise be copied into millions of shards.
pub(crate) const MAX_SHARD_SLOTS_PER_RECORD: i64 = 32;

/// A block a writer dropped, and enough information to find and identify it
/// again in a published index.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PendingRemoval {
    pub(crate) fingerprint: u64,
    pub(crate) min_ts: i64,
    pub(crate) max_ts: i64,
}

/// Maximum time from starting a merge attempt to publishing its manifest.
pub(crate) const SHARD_PAYLOAD_PUBLISH_TIMEOUT: Duration = Duration::from_mins(5);
/// How long an unreferenced shard payload is left alone before the sweep may
/// reclaim it.
pub(crate) const SHARD_PAYLOAD_SWEEP_GRACE: Duration = Duration::from_hours(1);
/// How often a snapshot write also sweeps orphaned payloads.
pub(crate) const SNAPSHOT_SWEEP_INTERVAL: u64 = 16;

/// What one save of a sharded index contributes, and where it goes: the
/// blocks this writer registered and the removals it owes the base.
pub(crate) struct SnapshotContribution<'a> {
    pub(crate) store: &'a Arc<dyn ObjectStore>,
    pub(crate) key: &'a str,
    pub(crate) removals: &'a BTreeMap<String, BTreeMap<String, PendingRemoval>>,
    pub(crate) additions: &'a BTreeSet<String>,
    pub(crate) max_bytes: ByteSize,
}

/// The shard that holds records whose spans cannot fit the bounded grid.
pub(crate) const UNBOUNDED_SHARD_RANGE: IndexShardRange = IndexShardRange::new(i64::MIN, i64::MAX);

pub const DEFAULT_INDEX_SNAPSHOT_MAX: ByteSize = mebibytes(256);

pub const DEFAULT_INDEX_SNAPSHOT_RETAIN: usize = 8;

/// Version of the snapshot-manifest encoding this build writes and accepts.
///
/// One version, no fallback: Krabka is greenfield, so a manifest written by an
/// older build is deleted, not migrated.
pub(crate) const SNAPSHOT_MANIFEST_VERSION: u32 = 1;

#[must_use]
pub fn index_snapshot_prefix_for_key(key: &str) -> String {
    let key = key.trim_matches('/');
    // Use a sibling prefix, not "{key}/snapshots": object stores allow that
    // shape, but filesystem-backed S3 services may already map the legacy key
    // to a directory containing retained physical object parts.
    if let Some(stem) = key.strip_suffix(".json") {
        format!("{stem}/snapshots")
    } else {
        format!("{key}.snapshots")
    }
}

pub async fn latest_index_snapshot_path(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Option<Path>> {
    Ok(list_index_snapshot_objects(store, key)
        .await?
        .pop()
        .map(|meta| meta.location))
}

#[instrument(level = "debug", skip_all, fields(key = %key), err)]
pub async fn list_index_snapshot_objects(
    store: &dyn ObjectStore,
    key: &str,
) -> Result<Vec<ObjectMeta>> {
    let prefix = Path::from(index_snapshot_prefix_for_key(key));
    let mut stream = store.list(Some(&prefix));
    let mut objects = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        if std::path::Path::new(meta.location.as_ref())
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        {
            objects.push(meta);
        }
    }
    objects.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
    Ok(objects)
}

#[instrument(level = "debug", skip_all, fields(key = %key, retain), err)]
pub(crate) async fn prune_old_index_snapshots(
    store: &dyn ObjectStore,
    key: &str,
    retain: usize,
) -> Result<()> {
    let objects = list_index_snapshot_objects(store, key).await?;
    let stale = objects.len().saturating_sub(retain);
    for meta in objects.into_iter().take(stale) {
        match store.delete(&meta.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

/// Writes one shard payload under the key its content hash names.
///
/// The key is a function of the bytes, so the write is idempotent and can
/// never overwrite a payload some other generation still names: encoding the
/// same shard twice puts the same bytes at the same key, and encoding a
/// different shard puts different bytes at a different key. `content` is the
/// hash the caller already computed to decide whether the write was needed at
/// all.
pub(crate) async fn put_shard_payload(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    tenant: &str,
    range: IndexShardRange,
    content: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    let object_key = shard_payload_object_key(key, tenant, range, content);
    store
        .put(&Path::from(object_key), PutPayload::from(bytes))
        .await?;
    Ok(())
}

/// The manifest a reader should answer from, or `None` when the key has no
/// generation yet.
///
/// # Errors
/// Returns an error when object-store I/O fails or the manifest is malformed.
pub(crate) async fn read_latest_snapshot_manifest(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    max_bytes: ByteSize,
    label: &str,
) -> Result<Option<SnapshotManifest>> {
    let Some(path) = latest_index_snapshot_path(store, key).await? else {
        return Ok(None);
    };
    match read_index_snapshot_bytes(store, &path, max_bytes, label).await? {
        IndexSnapshotBytes::Present(bytes) => {
            Ok(Some(SnapshotManifest::from_bytes(label, &bytes)?))
        }
        IndexSnapshotBytes::Absent(missing) => Err(missing),
    }
}

/// Content hash that names a shard payload object.
///
/// The name being a function of the bytes is what makes a payload immutable
/// and a write idempotent: two writers that encode the same shard mint the
/// same key and their puts are interchangeable, while a writer that changed
/// the shard mints a different key and cannot clobber the payload an older
/// retained manifest still names.
///
/// The hash is 128 bits rather than 64 because a collision here does not
/// corrupt one read, it silently serves one shard's index as another's. At a
/// hundred million live payloads, 128 bits leaves a collision probability
/// around 1e-23; 64 bits would leave it around 3e-4.
#[must_use]
pub(crate) fn shard_payload_content_hash(bytes: &[u8]) -> String {
    format!("{:032x}", xxh3_128(bytes))
}

/// Object key of one shard payload.
///
/// The tenant and the span are path segments so that the sweep can tell a
/// payload from anything else under the prefix without reading it, and so that
/// a listing is grouped the way a tenant-scoped load wants it. The tenant
/// carries an escape, which is what holds it inside its own segment. The
/// content hash is the filename, which is what keeps a payload immutable: a
/// changed shard is a new object, never an overwrite of one an older retained
/// manifest names.
#[must_use]
pub(crate) fn shard_payload_object_key(
    key: &str,
    tenant: &str,
    range: IndexShardRange,
    content: &str,
) -> String {
    format!(
        "{}/tenant={}/time={}-{}/{content}.kbs",
        shard_payload_prefix_for_key(key),
        escape_object_path_segment(tenant),
        shard_bound_key(range.start),
        shard_bound_key(range.end),
    )
}

/// Object key for one generation of an index snapshot.
///
/// The generation is a plain counter, and the next writer derives its key from
/// the newest generation it can see, so two writers that start from the same
/// state mint the *same* key and a conditional create lets only one of them
/// through. Zero-padding to 20 digits, `u64`'s widest decimal form, keeps the
/// lexicographic order of the keys equal to the numeric order of the
/// generations, so listing the prefix and taking the last entry still finds
/// the newest snapshot.
pub(crate) fn snapshot_key_for_generation(key: &str, generation: u64) -> String {
    format!(
        "{}/{generation:020}.json",
        index_snapshot_prefix_for_key(key)
    )
}

/// The slots a merge has to read: every one the span of a contributed block
/// or of a removal reaches into.
///
/// Every shard that meets one of these has to be read, because the record it
/// replaces or retires can only be in one of them.
pub(crate) fn touched_shard_ranges(
    contributed: impl IntoIterator<Item = IndexShardRange>,
    removed: Option<&BTreeMap<String, PendingRemoval>>,
    width: i64,
) -> Vec<IndexShardRange> {
    let removed = removed
        .into_iter()
        .flat_map(BTreeMap::values)
        .map(|removal| IndexShardRange::new(removal.min_ts, removal.max_ts));
    contributed
        .into_iter()
        .chain(removed)
        .flat_map(|span| shard_ranges_for_span(span.start, span.end, width))
        .collect()
}

/// Object prefix holding every shard payload of the index stored under `key`.
///
/// A sibling of the key, and a sibling of the snapshot prefix, for the reason
/// [`index_snapshot_prefix_for_key`] gives: a filesystem-backed S3
/// service may already have the key itself mapped to a directory.
#[must_use]
pub(crate) fn shard_payload_prefix_for_key(key: &str) -> String {
    let key = key.trim_matches('/');
    key.strip_suffix(".json").map_or_else(
        || format!("{key}.payloads"),
        |stem| format!("{stem}/payloads"),
    )
}

/// Reads back the generation that
/// [`snapshot_key_for_generation`] encoded.
pub(crate) fn snapshot_generation_from_path(path: &Path) -> Result<u64> {
    path.filename()
        .and_then(|filename| filename.strip_suffix(".json"))
        .and_then(|generation| generation.parse().ok())
        .ok_or_else(|| {
            BlockStoreError::InvalidBlock(format!(
                "index snapshot `{path}` is not named `<generation>.json`"
            ))
        })
}

/// The span of grid slot `slot`, clamped where the grid runs off the ends of
/// an `i64`.
pub(crate) fn shard_range_of_slot(slot: i64, width: i64) -> IndexShardRange {
    let range = shard_range(slot, width);
    IndexShardRange::new(range.start, range.end)
}

/// The shards a record spanning `[min_ts, max_ts]` belongs to.
///
/// A record belongs to every slot of the grid its own span crosses, so a
/// record that straddles a boundary is written into both, and a query that
/// reads two shards can see it twice. Callers merge records by object key, so
/// the duplicate costs nothing but the bytes. Cutting shards on record
/// boundaries instead is what would make an append rewrite its neighbours,
/// which is the cost this whole layout exists to avoid.
///
/// The grid width is fixed rather than derived from the tenant's span, which
/// the shared [`crate::Index`] does derive. Deriving it needs the tenant's
/// whole span, and reading the whole tenant to publish one shard is the thing
/// a manifest is here to stop.
pub(crate) fn shard_ranges_for_span(min_ts: i64, max_ts: i64, width: i64) -> Vec<IndexShardRange> {
    let width = width.max(1);
    let mut min_ts = min_ts;
    let mut max_ts = max_ts;
    if min_ts > max_ts {
        std::mem::swap(&mut min_ts, &mut max_ts);
    }
    let first = min_ts.div_euclid(width);
    let last = max_ts.div_euclid(width);
    let slots = bounded_shard_slots(first, last, MAX_SHARD_SLOTS_PER_RECORD);
    if slots.is_empty() {
        return vec![UNBOUNDED_SHARD_RANGE];
    }
    slots
        .into_iter()
        .map(|slot| shard_range_of_slot(slot, width))
        .collect()
}

/// Whether `location` is a shard payload of the index stored under `key`.
///
/// Anything that does not spell the shape [`super::shard_payload_object_key`]
/// writes is foreign, and the sweep neither reads nor deletes it. A tenant
/// segment whose escape does not read back is foreign too: the prefix
/// belongs to the index, but a bucket is shared and being wrong about that
/// costs somebody else their object.
pub(crate) fn is_shard_payload_location(key: &str, location: &str) -> bool {
    let prefix = shard_payload_prefix_for_key(key);
    let Some(rest) = location.strip_prefix(&prefix) else {
        return false;
    };
    let mut parts = rest.trim_start_matches('/').split('/');
    let Some(tenant) = parts.next() else {
        return false;
    };
    let Some(span) = parts.next() else {
        return false;
    };
    let Some(file) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    let is_tenant_segment = tenant
        .strip_prefix("tenant=")
        .is_some_and(|tenant| unescape_object_path_segment(tenant).is_some());
    if !is_tenant_segment
        || !std::path::Path::new(file)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("kbs"))
    {
        return false;
    }
    let Some(span) = span.strip_prefix("time=") else {
        return false;
    };
    // Both bounds are fixed-width, so the separator sits at a known offset and
    // there is no need to guess which `-` divides them.
    let Some(separator) = span.find('-') else {
        return false;
    };
    let Some(start) = parse_shard_bound_key(&span[..separator]) else {
        return false;
    };
    let Some(end) = parse_shard_bound_key(&span[separator + 1..]) else {
        return false;
    };
    start <= end
}

/// Outcome of reading a snapshot object.
///
/// An absent object is not always a failure: a first save starts from nothing,
/// and a merge base can be pruned out from under a reader. The error a strict
/// reader should report travels with the absence so that neither caller has to
/// re-derive it.
pub(crate) enum IndexSnapshotBytes {
    Present(Bytes),
    Absent(BlockStoreError),
}

/// Reads a snapshot object, capped at `max_bytes`.
///
/// `label` names the snapshot flavour in the error text.
pub(crate) async fn read_index_snapshot_bytes(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    max_bytes: ByteSize,
    label: &str,
) -> Result<IndexSnapshotBytes> {
    let error =
        match krabka_object_store::v013::read_capped(store, path, max_bytes.bytes_u64()).await {
            Ok(bytes) => return Ok(IndexSnapshotBytes::Present(bytes)),
            Err(error) => error,
        };
    if let Some(oversized) = oversized_object_error(CappedObject { label, name: path }, &error) {
        return Err(oversized);
    }
    if let krabka_object_store::v013::ObjectStoreError::NotFound(_) = error {
        return match store.head(path).await {
            // The object is there after all, so the read is the failure, not
            // the absence.
            Ok(_) => Err(BlockStoreError::ObjectStore(error.to_string())),
            Err(missing @ object_store::Error::NotFound { .. }) => Ok(IndexSnapshotBytes::Absent(
                BlockStoreError::ObjectStore(missing.to_string()),
            )),
            Err(error) => Err(BlockStoreError::ObjectStore(error.to_string())),
        };
    }
    Err(capped_read_error(store, path, error).await)
}

/// Resolves the merge base for the next snapshot write.
///
/// `Ok(None)` means the manifest the listing named was pruned before it could
/// be read, so the chain moved on under us and the caller should look again.
pub(crate) async fn read_manifest_snapshot_base(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    max_bytes: ByteSize,
    label: &str,
) -> Result<Option<ManifestSnapshotBase>> {
    let Some(meta) = list_index_snapshot_objects(store, key).await?.pop() else {
        // No generation exists yet. There is no chain and nothing to merge
        // into, and the first generation is zero.
        return Ok(Some(ManifestSnapshotBase {
            manifest: None,
            next_generation: 0,
        }));
    };
    let generation = snapshot_generation_from_path(&meta.location)?;
    let IndexSnapshotBytes::Present(bytes) =
        read_index_snapshot_bytes(store, &meta.location, max_bytes, label).await?
    else {
        return Ok(None);
    };
    Ok(Some(ManifestSnapshotBase {
        manifest: Some(SnapshotManifest::from_bytes(label, &bytes)?),
        next_generation: generation.saturating_add(1),
    }))
}

/// Reads one shard payload, refusing to buffer more than `max_bytes` of it.
///
/// The cap is per payload rather than per index, which is the tighter bound of
/// the two: a corrupt or hostile object under a shared prefix can no longer be
/// as large as a whole fleet index and still be read.
pub(crate) async fn read_shard_payload(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<Bytes> {
    let path = Path::from(object_key);
    match krabka_object_store::v013::read_capped(store, &path, max_bytes.bytes_u64()).await {
        Ok(bytes) => Ok(bytes),
        Err(error) => Err(
            match oversized_object_error(
                CappedObject {
                    label: "index shard payload",
                    name: &object_key,
                },
                &error,
            ) {
                Some(oversized) => oversized,
                None => capped_read_error(store, &path, error).await,
            },
        ),
    }
}

/// The one tenant, and the span of it, a windowed load keeps.
#[derive(Clone, Copy)]
pub(crate) struct ShardWindow<'a> {
    pub(crate) tenant: &'a str,
    pub(crate) span: IndexShardRange,
}

/// A load of the shard payloads one manifest names.
pub(crate) struct ManifestRead<'a> {
    pub(crate) store: &'a Arc<dyn ObjectStore>,
    pub(crate) key: &'a str,
    pub(crate) manifest: &'a SnapshotManifest,
    /// When given, only this tenant's shards that meet the span are read;
    /// everything else is listed in the manifest and then not read.
    pub(crate) window: Option<ShardWindow<'a>>,
    pub(crate) max_bytes: ByteSize,
}

/// A load of the latest published generation, which reads as an empty index
/// when nothing has been published yet.
pub(crate) struct LatestSnapshotRead<'a> {
    pub(crate) store: &'a Arc<dyn ObjectStore>,
    pub(crate) key: &'a str,
    /// When given, only this tenant's shards that meet the span are read.
    pub(crate) window: Option<ShardWindow<'a>>,
    pub(crate) max_bytes: ByteSize,
}

/// How many shards a load listed and how many it read.
pub(crate) struct WindowShardReads {
    pub(crate) listed: usize,
    pub(crate) read: usize,
}

impl ManifestRead<'_> {
    /// Reads the payloads in the window and hands each to `fold` with its
    /// tenant and object key.
    pub(crate) async fn read_shards(
        &self,
        mut fold: impl FnMut(&str, &str, &[u8]) -> Result<()>,
    ) -> Result<WindowShardReads> {
        let mut reads = WindowShardReads { listed: 0, read: 0 };
        for tenant in self.manifest.tenants() {
            if self.window.is_some_and(|window| window.tenant != tenant) {
                continue;
            }
            for shard in self.manifest.shards_of(tenant) {
                reads.listed += 1;
                let range = shard.range();
                if self
                    .window
                    .is_some_and(|window| !range.overlaps(window.span.start, window.span.end))
                {
                    continue;
                }
                let object_key = shard_payload_object_key(self.key, tenant, range, &shard.content);
                let bytes = read_shard_payload(self.store, &object_key, self.max_bytes).await?;
                reads.read += 1;
                fold(tenant, &object_key, &bytes)?;
            }
        }
        Ok(reads)
    }
}

/// Bound on optimistic retries within one snapshot write.
///
/// A losing attempt always means another writer's manifest landed, so the loop
/// only spins while the system as a whole makes progress. The bound turns a
/// store that never reports a conflict into a loud error instead of a hang.
const MAX_SNAPSHOT_WRITE_ATTEMPTS: usize = 64;

struct PublicationAttempt {
    len: usize,
    result: std::result::Result<object_store::PutResult, object_store::Error>,
}

/// Folds a writer's index into the newest published manifest and swaps the
/// result in as the next generation.
///
/// The generation swaps the *manifest*, not the index. `merge` is handed the
/// manifest the attempt observed, writes whatever shard payloads its own
/// contribution changed, and returns the manifest that names them beside the
/// shards it did not touch. Payloads are content-addressed and immutable, so a
/// losing attempt leaves nothing to undo: its payloads are either named by the
/// manifest it retries with, or swept later as orphans.
///
/// The write is a conditional create on a key derived from the generation it
/// merged, so two writers that read the same base contend for the same key and
/// exactly one wins. The loser re-reads the winner's manifest and merges again,
/// which is what keeps a concurrent writer's blocks in the index instead of
/// silently overwriting them.
#[instrument(
    skip_all,
    fields(key = %key, retain = %retain, attempts = tracing::field::Empty, len = tracing::field::Empty),
    err
)]
pub(crate) async fn put_manifest_snapshot<Merge, Fut>(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    retain: IndexSnapshotRetain,
    max_bytes: ByteSize,
    label: &str,
    merge: Merge,
) -> Result<String>
where
    Merge: Fn(Option<SnapshotManifest>) -> Fut,
    Fut: Future<Output = Result<SnapshotManifest>>,
{
    for attempt in 1..=MAX_SNAPSHOT_WRITE_ATTEMPTS {
        let Some(base) = read_manifest_snapshot_base(store, key, max_bytes, label).await? else {
            tokio::task::yield_now().await;
            continue;
        };
        let generation = base.next_generation;
        let snapshot_key = snapshot_key_for_generation(key, generation);
        let publication = tokio::time::timeout(SHARD_PAYLOAD_PUBLISH_TIMEOUT, async {
            let bytes = merge(base.manifest).await?.to_bytes()?;
            let len = bytes.len();
            let put = store
                .put_opts(
                    &Path::from(snapshot_key.clone()),
                    PutPayload::from(bytes),
                    PutOptions::from(PutMode::Create),
                )
                .await;
            Ok::<_, BlockStoreError>(PublicationAttempt { len, result: put })
        })
        .await
        .map_err(|_| {
            BlockStoreError::ObjectStore(format!(
                "index snapshot `{key}` publication exceeded {SHARD_PAYLOAD_PUBLISH_TIMEOUT:?}"
            ))
        })??;
        match publication.result {
            Ok(_) => {
                tracing::Span::current().record("attempts", attempt);
                tracing::Span::current().record("len", publication.len);
                prune_old_index_snapshots(store, key, retain.into_value()).await?;
                if generation.is_multiple_of(SNAPSHOT_SWEEP_INTERVAL) {
                    sweep_orphan_shard_payloads(
                        store,
                        key,
                        max_bytes,
                        label,
                        SHARD_PAYLOAD_SWEEP_GRACE,
                    )
                    .await?;
                }
                return Ok(snapshot_key);
            }
            // Another writer claimed this generation. Its manifest is now the
            // merge base, so re-read and fold into that instead.
            Err(object_store::Error::AlreadyExists { .. }) => tokio::task::yield_now().await,
            Err(error) => return Err(error.into()),
        }
    }

    Err(BlockStoreError::ObjectStore(format!(
        "index snapshot `{key}` lost {MAX_SNAPSHOT_WRITE_ATTEMPTS} consecutive writes to concurrent writers"
    )))
}
