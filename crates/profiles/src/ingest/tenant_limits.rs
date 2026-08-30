use super::*;

/// Per-tenant ingest limits for structural validation.
///
/// Not `Eq`: the label caps are [`ByteSize`] quantities, which store `f64`.
/// These limits are only ever a map value in `TenantLimitConfig::tenants`, so
/// nothing needs the derive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantLimits {
    /// Cap on the UTF-8 bytes of a label name.
    #[serde(default = "default_max_label_name", with = "label_byte_limit")]
    pub max_label_name: ByteSize,
    pub max_label_names_per_series: usize,
    /// Cap on the UTF-8 bytes of a label value.
    #[serde(with = "label_byte_limit")]
    pub max_label_value: ByteSize,
    pub session_id_buckets: u64,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_label_name: default_max_label_name(),
            max_label_names_per_series: 30,
            max_label_value: bytes(2048),
            session_id_buckets: 1024,
        }
    }
}
