use super::{ ByteSize, Frequency, Time, TimeExt,
    kibibytes, per_sec};

/// Structural per-request limits enforced before WAL append.
#[derive(Clone, Debug, PartialEq)]
pub struct TenantLimits {
    pub max_label_name_len: ByteSize,
    pub max_label_value_len: ByteSize,
    pub max_samples_per_series: usize,
    pub max_series_per_request: usize,
    /// Accepted sample rate. A zero rate turns the ingestion rate limit off.
    pub ingestion_rate: Frequency,
    /// Samples the token bucket may hand out in one burst.
    pub ingestion_burst_size: usize,
    /// Accepted out-of-order ingest window. A negative extent removes the cap.
    pub out_of_order_time_window: Time,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_label_name_len: kibibytes(2),
            max_label_value_len: kibibytes(2),
            max_samples_per_series: 10_000,
            max_series_per_request: 100_000,
            ingestion_rate: per_sec(1_000_000),
            ingestion_burst_size: 1_000_000,
            out_of_order_time_window: Time::ZERO,
        }
    }
}
