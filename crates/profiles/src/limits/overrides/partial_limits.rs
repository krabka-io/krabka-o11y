use super::*;

// The Pyroscope-shaped runtime-overrides keys, in the units an operator writes
// them (profiles/sec, bytes, seconds). Tenant entries are intentionally partial:
// this is partial configuration, not old-schema compatibility — each entry
// overrides only the limit fields it names, `merge_over` lifts them into the
// dimensioned `Limits`, and unknown keys are rejected (see `RuntimeFile`).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PartialLimits {
    #[serde(default)]
    pub(crate) ingestion_rate_profiles_per_sec: Option<f64>,
    #[serde(default)]
    pub(crate) ingestion_burst_profiles: Option<u64>,
    #[serde(default)]
    pub(crate) max_series: Option<u64>,
    #[serde(default)]
    pub(crate) max_label_name_length: Option<u64>,
    #[serde(default)]
    pub(crate) max_label_value_length: Option<u64>,
    #[serde(default)]
    pub(crate) max_label_names_per_series: Option<u64>,
    #[serde(default)]
    pub(crate) max_flamegraph_nodes_default: Option<i64>,
    #[serde(default)]
    pub(crate) max_flamegraph_nodes_max: Option<i64>,
    #[serde(default)]
    pub(crate) max_query_length_secs: Option<u64>,
    #[serde(default)]
    pub(crate) max_session_id_cardinality: Option<u64>,
}

impl PartialLimits {
    /// Validate numeric ranges before the merge of the partial into full limits.
    ///
    /// Rejects the following, each with [`OverridesError::Invalid`]:
    /// - a non-finite (`NaN` or `inf`) or negative
    ///   `ingestion_rate_profiles_per_sec`,
    /// - a negative flamegraph node cap, either `max_flamegraph_nodes_default`
    ///   or `max_flamegraph_nodes_max`.
    ///
    /// The remaining caps are `u64` and therefore cannot be negative. Serde
    /// already rejects an out-of-range YAML literal for them at
    /// deserialization.
    pub(crate) fn validate(&self, tenant: &str) -> Result<(), OverridesError> {
        let invalid = |reason: &str| OverridesError::Invalid {
            tenant: tenant.to_string(),
            reason: reason.to_string(),
        };
        if let Some(rate) = self.ingestion_rate_profiles_per_sec
            && (!rate.is_finite() || rate < 0.0)
        {
            return Err(invalid(
                "ingestion_rate_profiles_per_sec must be finite and >= 0",
            ));
        }
        if let Some(nodes) = self.max_flamegraph_nodes_default
            && nodes < 0
        {
            return Err(invalid("max_flamegraph_nodes_default must be >= 0"));
        }
        if let Some(nodes) = self.max_flamegraph_nodes_max
            && nodes < 0
        {
            return Err(invalid("max_flamegraph_nodes_max must be >= 0"));
        }
        Ok(())
    }

    pub(crate) fn merge_over(self, defaults: &Limits) -> Limits {
        Limits {
            ingestion_rate: self
                .ingestion_rate_profiles_per_sec
                .map_or(defaults.ingestion_rate, Frequency::from_per_sec),
            ingestion_burst_profiles: self
                .ingestion_burst_profiles
                .unwrap_or(defaults.ingestion_burst_profiles),
            max_series: self.max_series.unwrap_or(defaults.max_series),
            max_label_name: self
                .max_label_name_length
                .map_or(defaults.max_label_name, ByteSize::from_bytes),
            max_label_value: self
                .max_label_value_length
                .map_or(defaults.max_label_value, ByteSize::from_bytes),
            max_label_names_per_series: self
                .max_label_names_per_series
                .unwrap_or(defaults.max_label_names_per_series),
            max_flamegraph_nodes_default: self
                .max_flamegraph_nodes_default
                .unwrap_or(defaults.max_flamegraph_nodes_default),
            max_flamegraph_nodes_max: self
                .max_flamegraph_nodes_max
                .unwrap_or(defaults.max_flamegraph_nodes_max),
            max_query_length: self
                .max_query_length_secs
                .map_or(defaults.max_query_length, |secs| {
                    Time::from_secs(i64::try_from(secs).unwrap_or(i64::MAX))
                }),
            max_session_id_cardinality: self
                .max_session_id_cardinality
                .unwrap_or(defaults.max_session_id_cardinality),
        }
    }
}
