use super::*;

#[derive(Clone, Copy)]
pub(crate) enum OverTimeFn {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    Stddev,
    Stdvar,
    Mad,
    First,
    Last,
    TsOfFirst,
    TsOfLast,
    TsOfMin,
    TsOfMax,
    Present,
}

impl OverTimeFn {
    pub(crate) fn preserves_metric_name(self) -> bool {
        matches!(self, Self::First | Self::Last)
    }
}
