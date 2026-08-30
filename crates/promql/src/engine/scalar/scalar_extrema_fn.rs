use super::*;

#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
pub(crate) enum ScalarExtremaFn {
    Max,
    Min,
}

#[cfg(feature = "experimental-functions")]
impl ScalarExtremaFn {
    pub(crate) fn apply(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Max => left.max(right),
            Self::Min => left.min(right),
        }
    }
}
