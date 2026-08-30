use super::*;

/// The upper edge of a heatmap's value axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct MaxValue(pub i64);
