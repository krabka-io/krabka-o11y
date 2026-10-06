use super::{ExtendedSelectorModifier, RangeSeries, Time};

pub(crate) struct RangeEval {
    pub(crate) enable_type_and_unit_labels: bool,
    pub(crate) series: Vec<RangeSeries>,
    pub(crate) end_ms: i64,
    pub(crate) range: Time,
    pub(crate) modifier: Option<ExtendedSelectorModifier>,
}
