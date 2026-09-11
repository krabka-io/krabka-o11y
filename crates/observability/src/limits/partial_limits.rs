use super::{ByteSize, Deserialize, Time};

/// A sparse override: every field an operator left out keeps the value it
/// merges over.
///
/// `deny_unknown_fields` makes a misspelled key a load failure. A silently
/// ignored `max_line_sizes` would leave the operator believing a limit was set.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PartialLimits {
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_line_size: Option<ByteSize>,
    #[serde(default)]
    pub(crate) max_label_names_per_series: Option<u64>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_label_name_length: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_label_value_length: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_time::deserialize"
    )]
    pub(crate) reject_old_samples_max_age: Option<Time>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_time::deserialize"
    )]
    pub(crate) creation_grace_period: Option<Time>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_ingest_body: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_time::deserialize"
    )]
    pub(crate) max_query_length: Option<Time>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_time::deserialize"
    )]
    pub(crate) max_query_lookback: Option<Time>,
    #[serde(default)]
    pub(crate) max_entries_limit_per_query: Option<u64>,
    #[serde(default)]
    pub(crate) max_query_series: Option<u64>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_query_read: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_byte_size::deserialize"
    )]
    pub(crate) max_query_string_bytes: Option<ByteSize>,
    #[serde(
        default,
        deserialize_with = "super::option_non_negative_time::deserialize"
    )]
    pub(crate) max_query_range: Option<Time>,
}
