use super::{Display, From, Into};

/// The denominator of a reduced quantile fraction, for example `4` in `3/4`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct QuantileDenominator(pub u64);
