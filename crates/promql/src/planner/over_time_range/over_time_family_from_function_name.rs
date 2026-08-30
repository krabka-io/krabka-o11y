use super::OverTimeFamily;

/// Resolves a matrix-selector `*_over_time` function name to its [`OverTimeFamily`].
///
/// [`OverTimeFamily`] is the operator path, the float UDF chain. This function
/// returns `None` for the experimental members `mad_over_time`,
/// `first_over_time`, and the `ts_of_*_over_time` family. Those members have no
/// operator-leaf UDF and route through the engine's shared
/// `apply_outer_range_fn` kernel instead. This function also returns `None` for
/// any function outside the `*_over_time` set.
#[must_use]
pub fn over_time_family_from_function_name(name: &str) -> Option<OverTimeFamily> {
    match name {
        "sum_over_time" => Some(OverTimeFamily::Sum),
        "avg_over_time" => Some(OverTimeFamily::Avg),
        "count_over_time" => Some(OverTimeFamily::Count),
        "min_over_time" => Some(OverTimeFamily::Min),
        "max_over_time" => Some(OverTimeFamily::Max),
        "stddev_over_time" => Some(OverTimeFamily::Stddev),
        "stdvar_over_time" => Some(OverTimeFamily::Stdvar),
        "last_over_time" => Some(OverTimeFamily::Last),
        "present_over_time" => Some(OverTimeFamily::Present),
        "quantile_over_time" => Some(OverTimeFamily::Quantile),
        _ => None,
    }
}
