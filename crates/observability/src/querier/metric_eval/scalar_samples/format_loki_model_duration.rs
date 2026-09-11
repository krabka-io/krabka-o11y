use super::{Time, TimeExt};

/// Renders an extent the way `Loki` spells a *limit* in its own error text.
///
/// This is Prometheus `model.Duration`'s own format, which `Loki` uses for the
/// limit side of `ErrQueryTooLong`: it writes days, then hours, then minutes,
/// then seconds, and it skips a unit whose count is zero. The default cap of
/// 721 hours reads as `30d1h`, not as `721h0m0s`.
///
/// It is deliberately not [`format_loki_query_length`](super::format_loki_query_length),
/// which renders the *observed* side of the same message. `Loki` formats the
/// two sides with two different Go types, and the differential suite compares
/// the rendered text.
pub(crate) fn format_loki_model_duration(extent: Time) -> String {
    let mut remaining = extent.nanos_i64().max(0) / 1_000_000_000;
    if remaining == 0 {
        return "0s".to_string();
    }
    let mut rendered = String::new();
    for (unit, size) in [("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1)] {
        let count = remaining / size;
        if count > 0 {
            rendered.push_str(&count.to_string());
            rendered.push_str(unit);
            remaining -= count * size;
        }
    }
    rendered
}
