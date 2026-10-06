use krabka_domain_macros::EnumName;

use super::{
    Extremum, fold_extremum, last_value_by_timestamp, over_time_mean, over_time_sum,
    over_time_variance, quantile_value,
};

/// Which `*_over_time` function an [`OverTimeUdf`] evaluates.
///
/// Only the non-experimental, float-typed members that the operator path
/// supports appear here. `mad_over_time`, `first_over_time`, and the
/// `ts_of_*_over_time` family stay on the interpreter.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, EnumName)]
#[enum_name(accessor = "udf_name")]
pub enum OverTimeFamily {
    /// Sum of the window's sample values.
    #[name(value = "prom_sum_over_time")]
    Sum,
    /// Arithmetic mean of the window's sample values.
    #[name(value = "prom_avg_over_time")]
    Avg,
    /// Count of samples in the window.
    #[name(value = "prom_count_over_time")]
    Count,
    /// Smallest sample value. Prometheus folds NaN out, the same as the engine
    /// and the `prom_min` aggregate.
    #[name(value = "prom_min_over_time")]
    Min,
    /// Largest sample value. Prometheus folds NaN out, the same as the engine
    /// and the `prom_max` aggregate.
    #[name(value = "prom_max_over_time")]
    Max,
    /// Population standard deviation of the window's sample values.
    #[name(value = "prom_stddev_over_time")]
    Stddev,
    /// Population variance of the window's sample values.
    #[name(value = "prom_stdvar_over_time")]
    Stdvar,
    /// Value of the latest (max-timestamp) sample in the window.
    #[name(value = "prom_last_over_time")]
    Last,
    /// `1.0` if the window holds any sample.
    #[name(value = "prom_present_over_time")]
    Present,
    /// `phi`-quantile of the window's sample values, with linear interpolation.
    /// This matches the engine's `quantile_value`.
    #[name(value = "prom_quantile_over_time")]
    Quantile,
}

impl OverTimeFamily {
    /// Returns true if this family takes a leading `phi` quantile scalar argument.
    pub(crate) fn takes_quantile_param(self) -> bool {
        matches!(self, Self::Quantile)
    }

    /// Evaluates one window's reduction.
    ///
    /// `timestamps` and `values` are paired 1:1 in sample order. `phi` is the
    /// quantile for [`OverTimeFamily::Quantile`], and other families ignore it.
    /// This function returns `None` for an empty window, where Prometheus gives
    /// no value.
    pub(crate) fn eval_window(self, timestamps: &[i64], values: &[f64], phi: f64) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        let value = match self {
            Self::Sum => over_time_sum(values),
            Self::Avg => over_time_mean(values),
            Self::Count => values.iter().map(|_| 1.0).sum(),
            Self::Min => fold_extremum(values, Extremum::Min),
            Self::Max => fold_extremum(values, Extremum::Max),
            Self::Stddev => over_time_variance(values).sqrt(),
            Self::Stdvar => over_time_variance(values),
            Self::Last => last_value_by_timestamp(timestamps, values)?,
            Self::Present => 1.0,
            Self::Quantile => quantile_value(phi, values)?,
        };
        Some(value)
    }
}
