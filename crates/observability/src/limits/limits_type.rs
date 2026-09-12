use super::{
    ByteSize, ByteSizeExt, Deserialize, Serialize, Time, TimeExt, bytes, days, minutes, secs,
};

/// One tenant's complete limit set.
///
/// The key names and the defaults are `Loki`'s, from `limits_config` in
/// `pkg/validation/limits.go` at the 3.5.1 tag the `loki_differential` suite
/// pins. Three fields are Krabka's own and say so. Zero turns a limit off,
/// which is `Loki`'s sentinel for every one of these.
///
/// It is not `Eq`. Every dimensioned field stores `f64`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// Largest accepted log line. `Loki` default: `256KB`.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "super::non_negative_byte_size::deserialize"
    )]
    pub max_line_size: ByteSize,

    /// Most label names one stream may carry. `Loki` default: `15`.
    pub max_label_names_per_series: u64,

    /// Longest accepted label name. `Loki` default: `1024` bytes.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "super::non_negative_byte_size::deserialize"
    )]
    pub max_label_name_length: ByteSize,

    /// Longest accepted label value. `Loki` default: `2048` bytes.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "super::non_negative_byte_size::deserialize"
    )]
    pub max_label_value_length: ByteSize,

    /// Oldest accepted entry timestamp, measured back from now. `Loki`
    /// default: `7d`.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "super::non_negative_time::deserialize"
    )]
    pub reject_old_samples_max_age: Time,

    /// Furthest accepted entry timestamp into the future. `Loki` default:
    /// `10m`.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "super::non_negative_time::deserialize"
    )]
    pub creation_grace_period: Time,

    /// Largest accepted ingest request body.
    ///
    /// Krabka's own limit. `Loki` bounds a push body with its HTTP server
    /// settings and not with `limits_config`, so there is no upstream key to
    /// take. Default: `0`, that is no cap.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "super::non_negative_byte_size::deserialize"
    )]
    pub max_ingest_body: ByteSize,

    /// Widest `[start, end]` window a query may cover. `Loki` default: `721h`,
    /// which `Loki` writes as `30d1h`.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "super::non_negative_time::deserialize"
    )]
    pub max_query_length: Time,

    /// How far back a query may reach. `Loki` default: `0s`, that is no cap.
    ///
    /// `Loki` clamps the query start to this instead of refusing the query, and
    /// so does Krabka.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "super::non_negative_time::deserialize"
    )]
    pub max_query_lookback: Time,

    /// Largest `limit` parameter a query may ask for. `Loki` default: `5000`.
    pub max_entries_limit_per_query: u64,

    /// Most series one query may match. `Loki` default: `500`.
    pub max_query_series: u64,

    /// Largest summed size of the blocks one query plans to read. `Loki` calls
    /// this `max_query_bytes_read`, and its default is `0`, that is no cap.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "super::non_negative_byte_size::deserialize"
    )]
    pub max_query_read: ByteSize,

    /// Longest accepted `LogQL` query string, in bytes of source text.
    ///
    /// Krabka's own limit. It is deliberately not called `max_query_length`:
    /// `Loki` gives that key to the `[start, end]` window above, and one name
    /// for two meanings would mislead every operator who writes an overrides
    /// file. Default: `0`, that is no cap.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::byte_size::serialize",
        deserialize_with = "super::non_negative_byte_size::deserialize"
    )]
    pub max_query_string_bytes: ByteSize,

    /// A second, stricter cap on the `[start, end]` window a query may cover.
    ///
    /// Krabka's own limit, and a deliberate divergence. `Loki` has a key of
    /// this name, but it caps the `[range]` of a range selector, which Krabka
    /// does not limit. The effective window cap is the smaller of this and
    /// `max_query_length`. Default: `0`, that is no cap.
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "super::non_negative_time::deserialize"
    )]
    pub max_query_range: Time,

    /// How long a tenant's log blocks are kept. `Loki` default: `0s`.
    ///
    /// Zero keeps every block forever. It does **not** mean "delete
    /// everything": a tenant that configures nothing keeps every block it ever
    /// wrote, which is what `Loki` does with `retention_period: 0s`. Reading
    /// it the other way round would delete the whole history of every
    /// unconfigured tenant on the first sweep. A negative window is read the
    /// same way as zero.
    ///
    /// The compactor's retention sweep reads this window through
    /// [`krabka_blockstore::RetentionWindows`].
    #[serde(
        serialize_with = "krabka_units::serde_units::human::time::serialize",
        deserialize_with = "super::non_negative_time::deserialize"
    )]
    pub retention_period: Time,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            // `_ = l.MaxLineSize.Set("256KB")`, and `KB` is 1000 bytes there.
            max_line_size: bytes(256_000),
            // `validation.max-label-names-per-series`
            max_label_names_per_series: 15,
            // `validation.max-length-label-name`
            max_label_name_length: bytes(1024),
            // `validation.max-length-label-value`
            max_label_value_length: bytes(2048),
            // `_ = l.RejectOldSamplesMaxAge.Set("7d")`
            reject_old_samples_max_age: days(7),
            // `_ = l.CreationGracePeriod.Set("10m")`
            creation_grace_period: minutes(10),
            // Krabka's own, and off until an operator asks for it.
            max_ingest_body: ByteSize::ZERO,
            // `_ = l.MaxQueryLength.Set("721h")`, which is 2_595_600 seconds.
            max_query_length: secs(2_595_600),
            // `_ = l.MaxQueryLookback.Set("0s")`
            max_query_lookback: Time::ZERO,
            // `validation.max-entries-limit`
            max_entries_limit_per_query: 5_000,
            // `querier.max-query-series`
            max_query_series: 500,
            // `frontend.max-query-bytes-read`, which defaults to no cap.
            max_query_read: ByteSize::ZERO,
            // Krabka's own, and off until an operator asks for it.
            max_query_string_bytes: ByteSize::ZERO,
            // Krabka's own, and off until an operator asks for it.
            max_query_range: Time::ZERO,
            // `_ = l.RetentionPeriod.Set("0s")`
            retention_period: Time::ZERO,
        }
    }
}

impl Limits {
    /// A set with every limit turned off.
    ///
    /// [`distributor_router`](crate::distributor_router) builds one. It takes a
    /// WAL sink and nothing else, so there is no operator configuration for it
    /// to read a limit from, and a router that refused a push against a limit
    /// nobody set would be surprising.
    ///
    /// [`QuerierState::new`](crate::QuerierState::new) does not. It starts on
    /// [`Limits::default`], because `Loki` applies its own `max_query_length`
    /// to every read whether an operator configured one or not.
    #[must_use]
    pub fn unenforced() -> Self {
        Self {
            max_line_size: ByteSize::ZERO,
            max_label_names_per_series: 0,
            max_label_name_length: ByteSize::ZERO,
            max_label_value_length: ByteSize::ZERO,
            reject_old_samples_max_age: Time::ZERO,
            creation_grace_period: Time::ZERO,
            max_ingest_body: ByteSize::ZERO,
            max_query_length: Time::ZERO,
            max_query_lookback: Time::ZERO,
            max_entries_limit_per_query: 0,
            max_query_series: 0,
            max_query_read: ByteSize::ZERO,
            max_query_string_bytes: ByteSize::ZERO,
            max_query_range: Time::ZERO,
            retention_period: Time::ZERO,
        }
    }
}
