//! Compact, time-partitioned references to the authoritative `.index` objects.
//!
//! Catalog entries are written after manifests and before WAL acknowledgement.
//! A missing manifest is retired. The catalog never makes a block live by itself.

use krabka_blockstore::escape_object_path_segment;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use serde::{Deserialize, Serialize};

use crate::{CompactionIndexError, CompactionIndexManifest, MetricBlockKind};

const VERSION: u32 = 1;
// Ten radix-16 levels partition signed millisecond time into 2^24 ms leaves.
// The small fanout bounds LIST requests even for open-ended windows.
const LEAF_SHIFT: u32 = 24;
const TREE_MAX: u64 = (1_u64 << 40) - 1;

/// A manifest's bounds without its fingerprints or series labels.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricManifestReference {
    pub index_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub kind: MetricBlockKind,
}

impl From<&CompactionIndexManifest> for MetricManifestReference {
    fn from(manifest: &CompactionIndexManifest) -> Self {
        Self {
            index_key: manifest.index_key.clone(),
            min_ts: manifest.min_ts,
            max_ts: manifest.max_ts,
            kind: manifest.kind,
        }
    }
}

impl MetricManifestReference {
    /// Whether these bounds overlap the query. Metadata matches every range.
    #[must_use]
    pub fn overlaps(&self, start_ms: i64, end_ms: i64) -> bool {
        self.kind == MetricBlockKind::Metadata || (self.max_ts >= start_ms && self.min_ts <= end_ms)
    }

    /// The import marker, if this reference belongs to an imported TSDB block.
    #[must_use]
    pub fn publication_marker(&self) -> Option<String> {
        let (directory, _) = self.index_key.rsplit_once('/')?;
        crate::tsdb_import::TsdbImportKeys::is_import_directory(directory)
            .then(|| format!("{directory}/_published"))
    }
}

/// A versioned catalog record. Import publication markers hold all references
/// of the import, so a partial catalog listing cannot expose half an import.
#[derive(Debug, Deserialize, Serialize)]
pub struct MetricManifestCatalog {
    format_version: u32,
    pub manifests: Vec<MetricManifestReference>,
}

impl MetricManifestCatalog {
    /// Builds a versioned catalog from compact manifest references.
    #[must_use]
    pub fn new(manifests: Vec<MetricManifestReference>) -> Self {
        Self {
            format_version: VERSION,
            manifests,
        }
    }

    /// Encodes the compact references as JSON.
    ///
    /// # Errors
    /// Returns [`CompactionIndexError::Encode`] if JSON serialization fails.
    pub fn encode(&self) -> Result<Vec<u8>, CompactionIndexError> {
        serde_json::to_vec(self).map_err(|error| CompactionIndexError::Encode(error.to_string()))
    }

    /// Decodes a catalog of the current format version.
    ///
    /// # Errors
    /// Returns an error for malformed JSON or a missing or unsupported version.
    pub fn decode(bytes: &[u8]) -> Result<Self, CompactionIndexError> {
        let catalog: Self = serde_json::from_slice(bytes)
            .map_err(|error| CompactionIndexError::Decode(error.to_string()))?;
        if catalog.format_version != VERSION {
            return Err(CompactionIndexError::Decode(format!(
                "manifest catalog version {}, expected {VERSION}",
                catalog.format_version
            )));
        }
        Ok(catalog)
    }
}

fn ordered_time(timestamp: i64) -> u64 {
    u64::from_be_bytes(timestamp.to_be_bytes()) ^ (1_u64 << 63)
}

fn root(prefix: &str, tenant: &str) -> String {
    let tenant = escape_object_path_segment(tenant);
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        format!("{tenant}/_catalog/v1/")
    } else {
        format!("{prefix}/{tenant}/_catalog/v1/")
    }
}

/// The immutable summary key of a manifest. Bounds in the filename let readers
/// discard non-overlapping summaries without a GET or a label allocation.
#[must_use]
pub fn metric_manifest_catalog_key(manifest: &CompactionIndexManifest) -> String {
    krabka_blockstore::metrics_manifest_catalog_key(
        &manifest.tenant,
        &manifest.index_key,
        manifest.min_ts,
        manifest.max_ts,
        manifest.kind == MetricBlockKind::Metadata,
    )
}

/// Publishes a compact reference after its authoritative manifest is durable.
///
/// # Errors
/// Returns an error if catalog encoding or the object-store write fails.
pub async fn write_metric_manifest_catalog(
    store: &dyn ObjectStore,
    manifest: &CompactionIndexManifest,
) -> Result<(), CompactionIndexError> {
    let catalog = MetricManifestCatalog::new(vec![manifest.into()]);
    store
        .put(
            &Path::from(metric_manifest_catalog_key(manifest)),
            PutPayload::from(catalog.encode()?),
        )
        .await
        .map_err(|error| CompactionIndexError::ObjectStore(error.to_string()))?;
    Ok(())
}

/// Disjoint prefixes that cover the requested time interval and its spanning
/// entries. Whole subtrees use one LIST; narrow windows visit only their paths.
#[must_use]
pub fn metric_manifest_catalog_prefixes(
    prefix: &str,
    tenant: &str,
    start_ms: i64,
    end_ms: i64,
) -> Vec<Path> {
    // The service also accepts a prefix already narrowed to this tenant.
    let tenant_suffix = format!("/{}", escape_object_path_segment(tenant));
    let prefix = prefix.trim_end_matches('/');
    let prefix = prefix.strip_suffix(&tenant_suffix).unwrap_or(prefix);
    let root = root(prefix, tenant);
    let mut prefixes = vec![Path::from(format!("{root}timeless/"))];
    if start_ms <= end_ms {
        cover(
            &format!("{root}time/"),
            0,
            TREE_MAX,
            ordered_time(start_ms) >> LEAF_SHIFT,
            ordered_time(end_ms) >> LEAF_SHIFT,
            &mut prefixes,
        );
    }
    prefixes
}

fn cover(directory: &str, first: u64, last: u64, start: u64, end: u64, prefixes: &mut Vec<Path>) {
    if start <= first && end >= last || first == last {
        prefixes.push(Path::from(directory));
        return;
    }
    prefixes.push(Path::from(format!("{directory}entries/")));
    let width = (last - first + 1) / 16;
    let low = (start.max(first) - first) / width;
    let high = (end.min(last) - first) / width;
    for child in low..=high {
        let child_first = first + child * width;
        cover(
            &format!("{directory}{child:x}/"),
            child_first,
            child_first + width - 1,
            start,
            end,
            prefixes,
        );
    }
}

/// Filters the compact bounds in a catalog filename before fetching its body.
#[must_use]
pub fn metric_manifest_catalog_key_overlaps(key: &str, start_ms: i64, end_ms: i64) -> bool {
    if key.contains("/_catalog/v1/timeless/") {
        return true;
    }
    let Some(name) = key
        .rsplit('/')
        .next()
        .and_then(|name| name.strip_suffix(".catalog"))
    else {
        return false;
    };
    let mut parts = name.split('-');
    let bounds = parts.next().zip(parts.next()).and_then(|(start, end)| {
        Some((
            u64::from_str_radix(start, 16).ok()?,
            u64::from_str_radix(end, 16).ok()?,
        ))
    });
    // Fetch malformed summaries so decoding reports corruption instead of hiding it.
    bounds.is_none_or(|(start, end)| end >= ordered_time(start_ms) && start <= ordered_time(end_ms))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_blockstore::BlockLevel;

    use super::*;

    fn manifest(min_ts: i64, max_ts: i64, kind: MetricBlockKind) -> CompactionIndexManifest {
        CompactionIndexManifest {
            tenant: "a/b%".to_owned(),
            kind,
            block_key: "metrics/a!2Fb!25/float/test.parquet".to_owned(),
            index_key: "metrics/a!2Fb!25/float/test.index".to_owned(),
            level: BlockLevel::INGESTED,
            first_offset: 0,
            last_offset: 0,
            row_count: 1,
            min_ts,
            max_ts,
            fingerprints: vec![],
            series: vec![],
        }
    }

    #[test]
    fn time_cover_reaches_each_overlapping_manifest_once_including_spanning_nodes() {
        let times = [
            i64::MIN,
            -8_589_934_592,
            -16_777_217,
            -1,
            0,
            1,
            16_777_215,
            16_777_216,
            8_589_934_592,
            i64::MAX,
        ];
        for (left_index, &start) in times.iter().enumerate() {
            for &end in &times[left_index..] {
                let prefixes = metric_manifest_catalog_prefixes("metrics", "a/b%", start, end);
                for (min_index, &min) in times.iter().enumerate() {
                    for &max in &times[min_index..] {
                        let manifest = manifest(min, max, MetricBlockKind::Float);
                        let key = metric_manifest_catalog_key(&manifest);
                        let hits = prefixes
                            .iter()
                            .filter(|prefix| Path::from(key.as_str()).prefix_matches(prefix))
                            .count();
                        if min <= end && max >= start {
                            assert!(hits == 1, "{min}..{max} queried at {start}..{end}: {key}");
                            assert!(metric_manifest_catalog_key_overlaps(&key, start, end));
                        } else {
                            assert!(!metric_manifest_catalog_key_overlaps(&key, start, end));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn metadata_is_timeless_and_tenant_prefixes_are_escaped() {
        let manifest = manifest(0, 0, MetricBlockKind::Metadata);
        let key = metric_manifest_catalog_key(&manifest);
        assert!(key.starts_with("metrics/a!2Fb!25/_catalog/v1/timeless/"));
        assert!(metric_manifest_catalog_key_overlaps(&key, 100, 200));
        let prefixes = metric_manifest_catalog_prefixes("metrics", "a/b%", 100, 200);
        assert!(
            prefixes
                .iter()
                .any(|prefix| Path::from(key.as_str()).prefix_matches(prefix))
        );
        assert!(prefixes == metric_manifest_catalog_prefixes("metrics/a!2Fb!25", "a/b%", 100, 200));
    }

    #[test]
    fn catalog_rejects_missing_and_unknown_versions() {
        assert!(MetricManifestCatalog::decode(br#"{"manifests":[]}"#).is_err());
        assert!(MetricManifestCatalog::decode(br#"{"format_version":2,"manifests":[]}"#).is_err());
        let catalog = MetricManifestCatalog::new(vec![]);
        assert!(
            MetricManifestCatalog::decode(&catalog.encode().unwrap())
                .unwrap()
                .manifests
                .is_empty()
        );
    }
}
