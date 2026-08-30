use super::format_loki_decimal_unit;

pub(crate) fn format_loki_offset_seconds(duration_ns: i64) -> String {
    format_loki_decimal_unit(duration_ns, 1_000_000_000, 9, "s")
}
