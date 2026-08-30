
/// Widens a signed count for a projected sample value.
///
/// `i64::to_f64` never fails, so the fallback is unreachable. It keeps the
/// conversion free of a lossy `as` cast.
pub(crate) fn widen(value: i64) -> f64 {
    num_traits::ToPrimitive::to_f64(&value).unwrap_or(f64::MAX)
}
