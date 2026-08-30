use super::{Limits, PartialLimits};

/// Overlays a sparse per-tenant override, or a defaults override, on top of
/// `base`.
///
/// Overrides are **fully trusted**. Any field set in `partial` replaces the
/// matching `base` value verbatim, with no floor and no hard cap. A value of `0`
/// is not rejected. For the limits that treat `0` as a sentinel, this *turns
/// off* that cap for the tenant. Those limits are `ingestion_rate`,
/// `max_global_series_per_user`, and the per-query, query-range, and lookback
/// caps. This matches Mimir's runtime-overrides semantics, where an
/// operator-supplied override is authoritative and `0` means "unlimited".
pub(crate) fn merge_limits(base: &Limits, partial: &PartialLimits) -> Limits {
    Limits {
        ingestion_rate: partial.ingestion_rate.unwrap_or(base.ingestion_rate),
        ingestion_burst_size: partial
            .ingestion_burst_size
            .unwrap_or(base.ingestion_burst_size),
        max_global_series_per_user: partial
            .max_global_series_per_user
            .unwrap_or(base.max_global_series_per_user),
        max_label_name_length: partial
            .max_label_name_length
            .unwrap_or(base.max_label_name_length),
        max_label_value_length: partial
            .max_label_value_length
            .unwrap_or(base.max_label_value_length),
        max_samples_per_query: partial
            .max_samples_per_query
            .unwrap_or(base.max_samples_per_query),
        max_fetched_series_per_query: partial
            .max_fetched_series_per_query
            .unwrap_or(base.max_fetched_series_per_query),
        max_query_lookback: partial
            .max_query_lookback
            .unwrap_or(base.max_query_lookback),
        max_query_length: partial.max_query_length.unwrap_or(base.max_query_length),
        out_of_order_time_window: partial
            .out_of_order_time_window
            .unwrap_or(base.out_of_order_time_window),
    }
}
