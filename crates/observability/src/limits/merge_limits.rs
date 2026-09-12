use super::{Limits, PartialLimits};

/// Overlays a sparse override, or the file's `defaults` block, on top of
/// `base`.
///
/// An override is **fully trusted**. Any field the operator set replaces the
/// matching `base` value exactly, with no floor and no ceiling, and a value of
/// `0` turns that limit off for the tenant. This is `Loki`'s own
/// runtime-overrides rule: an operator-supplied override is authoritative.
pub(crate) fn merge_limits(base: &Limits, partial: &PartialLimits) -> Limits {
    Limits {
        max_line_size: partial.max_line_size.unwrap_or(base.max_line_size),
        max_label_names_per_series: partial
            .max_label_names_per_series
            .unwrap_or(base.max_label_names_per_series),
        max_label_name_length: partial
            .max_label_name_length
            .unwrap_or(base.max_label_name_length),
        max_label_value_length: partial
            .max_label_value_length
            .unwrap_or(base.max_label_value_length),
        reject_old_samples_max_age: partial
            .reject_old_samples_max_age
            .unwrap_or(base.reject_old_samples_max_age),
        creation_grace_period: partial
            .creation_grace_period
            .unwrap_or(base.creation_grace_period),
        max_ingest_body: partial.max_ingest_body.unwrap_or(base.max_ingest_body),
        max_query_length: partial.max_query_length.unwrap_or(base.max_query_length),
        max_query_lookback: partial
            .max_query_lookback
            .unwrap_or(base.max_query_lookback),
        max_entries_limit_per_query: partial
            .max_entries_limit_per_query
            .unwrap_or(base.max_entries_limit_per_query),
        max_query_series: partial.max_query_series.unwrap_or(base.max_query_series),
        max_query_read: partial.max_query_read.unwrap_or(base.max_query_read),
        max_query_string_bytes: partial
            .max_query_string_bytes
            .unwrap_or(base.max_query_string_bytes),
        max_query_range: partial.max_query_range.unwrap_or(base.max_query_range),
        retention_period: partial.retention_period.unwrap_or(base.retention_period),
    }
}
