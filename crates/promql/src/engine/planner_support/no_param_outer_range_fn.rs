use super::{IrateFn, OuterRangeFn, OverTimeFn, RangeFn};

/// Maps a range or `*_over_time` function name to its [`OuterRangeFn`].
///
/// The map applies when the function takes exactly one argument, the range
/// vector. The parameterized functions
/// (`quantile_over_time`/`predict_linear`/`double_exponential_smoothing`) and
/// the non-fold helpers (`absent_over_time`/`time`/...) return `None` here, and
/// other code matches them separately.
pub(crate) fn no_param_outer_range_fn(name: &str) -> Option<OuterRangeFn> {
    Some(match name {
        "rate" => OuterRangeFn::Range(RangeFn::Rate),
        "increase" => OuterRangeFn::Range(RangeFn::Increase),
        "delta" => OuterRangeFn::Range(RangeFn::Delta),
        "changes" => OuterRangeFn::Range(RangeFn::Changes),
        "resets" => OuterRangeFn::Range(RangeFn::Resets),
        "irate" => OuterRangeFn::InstantDelta(IrateFn::Irate),
        "idelta" => OuterRangeFn::InstantDelta(IrateFn::Idelta),
        "deriv" => OuterRangeFn::Deriv,
        "sum_over_time" => OuterRangeFn::OverTime(OverTimeFn::Sum),
        "avg_over_time" => OuterRangeFn::OverTime(OverTimeFn::Avg),
        "count_over_time" => OuterRangeFn::OverTime(OverTimeFn::Count),
        "min_over_time" => OuterRangeFn::OverTime(OverTimeFn::Min),
        "max_over_time" => OuterRangeFn::OverTime(OverTimeFn::Max),
        "stddev_over_time" => OuterRangeFn::OverTime(OverTimeFn::Stddev),
        "stdvar_over_time" => OuterRangeFn::OverTime(OverTimeFn::Stdvar),
        "mad_over_time" => OuterRangeFn::OverTime(OverTimeFn::Mad),
        "first_over_time" => OuterRangeFn::OverTime(OverTimeFn::First),
        "last_over_time" => OuterRangeFn::OverTime(OverTimeFn::Last),
        "ts_of_first_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfFirst),
        "ts_of_last_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfLast),
        "ts_of_min_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfMin),
        "ts_of_max_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfMax),
        "present_over_time" => OuterRangeFn::OverTime(OverTimeFn::Present),
        _ => return None,
    })
}
