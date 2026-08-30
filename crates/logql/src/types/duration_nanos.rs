use super::*;

/// A range-selector window, in nanoseconds. This is the `[5m]` in a metric
/// query.
///
/// The type holds raw nanoseconds and not a `krabka_units::Time`. A
/// `krabka_units::Time` stores `f64` seconds, which is exact for integers below
/// 2^53 only, that is about 104 days of nanoseconds. `LogQL` admits `w` and `y`
/// duration literals well past that limit. A window written as
/// `1y2w3d4h5m6s7ms8us9ns` must round-trip to the nanosecond, and the field
/// filters compare durations exactly, so the value stays an integer.
/// `docs/uom-adoption.md` excludes nanosecond magnitudes for the same
/// reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct DurationNanos(pub i64);
