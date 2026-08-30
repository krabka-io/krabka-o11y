use super::{RangeSeries, Time, ExtendedSelectorModifier};

pub(crate) struct RangeEval {
    pub(crate) series: Vec<RangeSeries>,
    pub(crate) end_ms: i64,
    pub(crate) range: Time,
    pub(crate) modifier: Option<ExtendedSelectorModifier>,
}
