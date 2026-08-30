/// Prometheus' stale-NaN marker: the IEEE-754 quiet-NaN bit pattern that
/// Prometheus writes to end a series.
///
/// Instant-vector selection drops a series whose selected sample carries this
/// exact pattern. It keeps a genuine NaN value, which is any other NaN bit
/// pattern, as a NaN sample. Both the interpreter
/// (`engine::eval_instant_selector`) and the `InstantManipulate` operator route
/// staleness decisions through [`is_stale_nan`], so the two paths agree.
pub(crate) const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
