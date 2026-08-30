use super::*;

pub(crate) fn over_time_function(name: &str) -> Option<OverTimeFn> {
    Some(match name {
        "sum_over_time" => OverTimeFn::Sum,
        "avg_over_time" => OverTimeFn::Avg,
        "count_over_time" => OverTimeFn::Count,
        "min_over_time" => OverTimeFn::Min,
        "max_over_time" => OverTimeFn::Max,
        "stddev_over_time" => OverTimeFn::Stddev,
        "stdvar_over_time" => OverTimeFn::Stdvar,
        "mad_over_time" => OverTimeFn::Mad,
        "first_over_time" => OverTimeFn::First,
        "last_over_time" => OverTimeFn::Last,
        "ts_of_first_over_time" => OverTimeFn::TsOfFirst,
        "ts_of_last_over_time" => OverTimeFn::TsOfLast,
        "ts_of_min_over_time" => OverTimeFn::TsOfMin,
        "ts_of_max_over_time" => OverTimeFn::TsOfMax,
        "present_over_time" => OverTimeFn::Present,
        _ => return None,
    })
}
