use super::{Deserialize, Frequency, ByteSize, Time, serde_units};

#[derive(Debug, Default, Deserialize)]
pub(crate) struct PartialLimits {
    #[serde(default, with = "serde_units::human::option_frequency")]
    pub(crate) ingestion_rate: Option<Frequency>,
    #[serde(default)]
    pub(crate) ingestion_burst_size: Option<u64>,
    #[serde(default)]
    pub(crate) max_global_series_per_user: Option<u64>,
    #[serde(default, with = "serde_units::human::option_byte_size")]
    pub(crate) max_label_name_length: Option<ByteSize>,
    #[serde(default, with = "serde_units::human::option_byte_size")]
    pub(crate) max_label_value_length: Option<ByteSize>,
    #[serde(default)]
    pub(crate) max_samples_per_query: Option<u64>,
    #[serde(default)]
    pub(crate) max_fetched_series_per_query: Option<u64>,
    #[serde(
        default,
        deserialize_with = "super::super::option_non_negative_time::deserialize"
    )]
    pub(crate) max_query_lookback: Option<Time>,
    #[serde(
        default,
        deserialize_with = "super::super::option_non_negative_time::deserialize"
    )]
    pub(crate) max_query_length: Option<Time>,
    #[serde(default, with = "serde_units::human::option_time")]
    pub(crate) out_of_order_time_window: Option<Time>,
}
