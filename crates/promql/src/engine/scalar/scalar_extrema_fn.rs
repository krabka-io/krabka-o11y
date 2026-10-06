#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
pub(crate) enum ScalarExtremaFn {
    Max,
    Min,
}

#[cfg(feature = "experimental-functions")]
impl ScalarExtremaFn {
    pub(crate) fn apply(self, left: f64, right: f64) -> f64 {
        if left.is_nan() || right.is_nan() {
            return f64::NAN;
        }
        match self {
            Self::Max => left.max(right),
            Self::Min => left.min(right),
        }
    }
}
