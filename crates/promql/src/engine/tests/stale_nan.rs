
pub(crate) fn stale_nan() -> f64 {
    f64::from_bits(0x7ff0_0000_0000_0002)
}
