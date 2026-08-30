use super::*;

/// Per-tenant profile limits.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Pyroscope `ingestion_rate_mb` analog, counted in profiles per second.
    /// Zero means unlimited.
    #[serde(with = "krabka_units::serde_units::human::frequency")]
    pub ingestion_rate: Frequency,
    /// Pyroscope `ingestion_burst_size_mb` analog, counted in profiles.
    pub ingestion_burst_profiles: u64,
    /// Pyroscope `max_series`; `0` means unlimited.
    pub max_series: u64,
    /// Pyroscope `max_label_name_length`, a cap on the UTF-8 bytes of a label
    /// name; zero means unlimited.
    #[serde(with = "krabka_units::serde_units::human::byte_size")]
    pub max_label_name: ByteSize,
    /// Pyroscope `max_label_value_length`, a cap on the UTF-8 bytes of a label
    /// value; zero means unlimited.
    #[serde(with = "krabka_units::serde_units::human::byte_size")]
    pub max_label_value: ByteSize,
    /// Pyroscope `max_label_names_per_series`; `0` means unlimited.
    pub max_label_names_per_series: u64,
    /// Pyroscope `max_flamegraph_nodes_default`.
    pub max_flamegraph_nodes_default: i64,
    /// Pyroscope `max_flamegraph_nodes_max`; `0` means unlimited.
    pub max_flamegraph_nodes_max: i64,
    /// Pyroscope `max_query_length`, the `(end-start)` ceiling; zero means
    /// unlimited.
    #[serde(with = "krabka_units::serde_units::human::time")]
    pub max_query_length: Time,
    /// `__session_id__` modulo-hash bucket cap; `0` means unlimited.
    pub max_session_id_cardinality: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            ingestion_rate: per_sec(10_000),
            ingestion_burst_profiles: 10_000,
            max_series: 0,
            max_label_name: bytes(1024),
            max_label_value: bytes(2048),
            max_label_names_per_series: 40,
            max_flamegraph_nodes_default: 2048,
            max_flamegraph_nodes_max: 0,
            max_query_length: DEFAULT_MAX_QUERY_LENGTH,
            max_session_id_cardinality: 0,
        }
    }
}

impl Limits {
    #[must_use]
    pub fn effective_max_nodes(&self, requested: i64) -> i64 {
        let requested = if requested > 0 {
            requested
        } else {
            self.max_flamegraph_nodes_default
        };
        if self.max_flamegraph_nodes_max > 0 {
            requested.min(self.max_flamegraph_nodes_max)
        } else {
            requested
        }
    }

    ///
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn validate_query_range_ms(
        &self,
        start_ms: StartMs,
        end_ms: EndMs,
    ) -> Result<(), LimitError> {
        // The bounds are instants, so they stay epoch milliseconds; only their
        // difference is an extent. Pyroscope reports both sides of this limit in
        // whole seconds, rounded up, so the extent is ceilinged on the way out.
        let limit_secs = self.max_query_length.secs_f64().to_u64().unwrap_or(0);
        if limit_secs == 0 || end_ms.0 <= start_ms.0 {
            return Ok(());
        }
        let observed = Time::from_millis(end_ms.0.saturating_sub(start_ms.0));
        let observed_secs = observed.secs_f64().ceil().to_u64().unwrap_or(u64::MAX);
        if observed_secs > limit_secs {
            return Err(LimitError::QueryLengthExceeded {
                limit_secs,
                observed_secs,
            });
        }
        Ok(())
    }
}
