use super::*;

/// The numerator of a reduced quantile fraction, for example `3` in `3/4`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct QuantileNumerator(pub u64);
