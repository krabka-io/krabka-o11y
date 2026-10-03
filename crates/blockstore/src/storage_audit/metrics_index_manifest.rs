use super::{BlockLevel, Labels, MetricsBlockKind, SerdeCompat, WincodeDeserialize};

/// What the audit reads from a metrics `.index` manifest, which
/// `krabka-metrics` persists as `CompactionIndexManifest`.
#[derive(Debug)]
pub struct MetricsIndexManifest {
    pub tenant: String,
    pub block_key: String,
    pub index_key: String,
}

impl MetricsIndexManifest {
    /// Decodes a manifest the way `CompactionIndexManifest::decode` does.
    ///
    /// The blockstore does not depend on the metrics crate, so the shape is
    /// restated here. The codec is `serde-wincode`, which writes the fields of
    /// a struct in order and with no names. The tuple below therefore has the
    /// fields of that struct in their order and with their types: `tenant`,
    /// `kind`, `block_key`, `index_key`, `level`, `first_offset`,
    /// `last_offset`, `row_count`, `min_ts`, `max_ts`, `fingerprints`, and
    /// `series`. The audit decodes every field, so a short or malformed
    /// manifest does not decode.
    ///
    /// # Errors
    /// Returns the codec error for bytes that are not a whole manifest.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        type Shape = (
            String,
            MetricsBlockKind,
            String,
            String,
            BlockLevel,
            i64,
            i64,
            usize,
            i64,
            i64,
            Vec<u64>,
            Vec<(u64, Labels)>,
        );
        let (tenant, _, block_key, index_key, ..) =
            <SerdeCompat<Shape> as WincodeDeserialize>::deserialize(bytes)
                .map_err(|error| error.to_string())?;
        Ok(Self {
            tenant,
            block_key,
            index_key,
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
