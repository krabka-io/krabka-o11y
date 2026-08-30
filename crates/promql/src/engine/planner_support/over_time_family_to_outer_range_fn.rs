use super::{OuterRangeFn, OverTimeFamily, OverTimeFn};

/// Maps an [`OverTimeFamily`] to the shared [`OuterRangeFn`] of the same name.
///
/// [`OverTimeFamily`] is the output of the `*_over_time` matcher, and the
/// `eval_over_time_call` of the interpreter applies the [`OuterRangeFn`].
/// `quantile_over_time` carries its resolved `phi`. Every other member maps to
/// the matching [`OverTimeFn`]. The matcher returns only the non-experimental
/// members, so the experimental [`OverTimeFn`] variants (`Mad`/`First`/`TsOf*`)
/// are unreachable here.
pub(crate) fn over_time_family_to_outer_range_fn(family: OverTimeFamily, phi: f64) -> OuterRangeFn {
    match family {
        OverTimeFamily::Sum => OuterRangeFn::OverTime(OverTimeFn::Sum),
        OverTimeFamily::Avg => OuterRangeFn::OverTime(OverTimeFn::Avg),
        OverTimeFamily::Count => OuterRangeFn::OverTime(OverTimeFn::Count),
        OverTimeFamily::Min => OuterRangeFn::OverTime(OverTimeFn::Min),
        OverTimeFamily::Max => OuterRangeFn::OverTime(OverTimeFn::Max),
        OverTimeFamily::Stddev => OuterRangeFn::OverTime(OverTimeFn::Stddev),
        OverTimeFamily::Stdvar => OuterRangeFn::OverTime(OverTimeFn::Stdvar),
        OverTimeFamily::Last => OuterRangeFn::OverTime(OverTimeFn::Last),
        OverTimeFamily::Present => OuterRangeFn::OverTime(OverTimeFn::Present),
        OverTimeFamily::Quantile => OuterRangeFn::QuantileOverTime(phi),
    }
}
