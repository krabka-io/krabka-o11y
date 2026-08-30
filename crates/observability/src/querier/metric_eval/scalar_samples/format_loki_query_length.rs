use super::*;

/// Renders an extent the way `Loki` spells a query length in its own error text.
///
/// The whole seconds come from the nanosecond count by integer division, not
/// from [`TimeExt::secs_i64`]. That method rounds to nearest and would report a
/// second more than `Loki` does for the same window.
pub(crate) fn format_loki_query_length(range: Time) -> String {
    let total_seconds = range.nanos_i64().max(0) / 1_000_000_000;
    let hours = total_seconds / 3_600;
    let minutes = total_seconds % 3_600 / 60;
    let seconds = total_seconds % 60;

    format!("{hours}h{minutes}m{seconds}s")
}
