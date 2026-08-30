use super::{OverTimeFamily, ScalarUDF, over_time_udf};

/// Returns every non-experimental `*_over_time` UDF, ready to register on a
/// [`SessionContext`].
#[must_use]
pub fn over_time_family_udfs() -> Vec<ScalarUDF> {
    [
        OverTimeFamily::Sum,
        OverTimeFamily::Avg,
        OverTimeFamily::Count,
        OverTimeFamily::Min,
        OverTimeFamily::Max,
        OverTimeFamily::Stddev,
        OverTimeFamily::Stdvar,
        OverTimeFamily::Last,
        OverTimeFamily::Present,
        OverTimeFamily::Quantile,
    ]
    .into_iter()
    .map(over_time_udf)
    .collect()
}
