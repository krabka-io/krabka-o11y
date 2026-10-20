use serde::Deserialize;

use super::{BlockLevel, Labels, MetricsBlockKind, SerdeCompat, WincodeDeserialize};

#[derive(Deserialize)]
#[allow(dead_code)] // Fields validate the serialized manifest shape on decode.
struct MetricsManifestShape {
    tenant: String,
    kind: MetricsBlockKind,
    block_key: String,
    index_key: String,
    level: BlockLevel,
    first_offset: i64,
    last_offset: i64,
    row_count: usize,
    min_ts: i64,
    max_ts: i64,
    fingerprints: Vec<u64>,
    series: Vec<MetricsManifestSeries>,
}

#[derive(Deserialize)]
#[allow(dead_code)] // The series payload is checked as part of the manifest shape.
struct MetricsManifestSeries {
    fingerprint: u64,
    labels: Labels,
}

/// What the audit reads from a metrics `.index` manifest, which
/// `krabka-metrics` persists as `CompactionIndexManifest`.
#[derive(Clone, Debug)]
pub struct MetricsIndexManifest {
    pub kind: MetricsBlockKind,
    pub min_ts: i64,
    pub max_ts: i64,
    pub tenant: String,
    pub block_key: String,
    pub index_key: String,
}

impl MetricsIndexManifest {
    /// Decodes a manifest the way `CompactionIndexManifest::decode` does.
    ///
    /// The blockstore does not depend on the metrics crate, so the shape is
    /// restated here. The codec is `serde-wincode`, which writes the fields of
    /// a struct in order and with no names. This named shape therefore has the
    /// fields of that struct in their order and with their types: `tenant`,
    /// `kind`, `block_key`, `index_key`, `level`, `first_offset`,
    /// `last_offset`, `row_count`, `min_ts`, `max_ts`, `fingerprints`, and
    /// `series`. The audit decodes every field, so a short or malformed
    /// manifest does not decode.
    ///
    /// # Errors
    /// Returns the codec error for bytes that are not a whole manifest.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let shape = <SerdeCompat<MetricsManifestShape> as WincodeDeserialize>::deserialize(bytes)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            kind: shape.kind,
            min_ts: shape.min_ts,
            max_ts: shape.max_ts,
            tenant: shape.tenant,
            block_key: shape.block_key,
            index_key: shape.index_key,
        })
    }

    /// What is wrong when this manifest, read from `key`, does not describe
    /// `block` of `tenant`, the block and the tenant that `key` names.
    ///
    /// The metrics retention pass refuses a manifest whose index key is not
    /// the key it was read from. A manifest that names another block or
    /// tenant than its key describes a block that is not the one beside it.
    pub fn mismatch(&self, key: &str, block: &str, tenant: Option<&str>) -> Option<String> {
        if self.index_key != key {
            Some(format!("names index key `{}`", self.index_key))
        } else if self.block_key != block {
            Some(format!("names block `{}`", self.block_key))
        } else if tenant.is_some_and(|tenant| tenant != self.tenant) {
            Some(format!("names tenant `{}`", self.tenant))
        } else {
            None
        }
    }
}
