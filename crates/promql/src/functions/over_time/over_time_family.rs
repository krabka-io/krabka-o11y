use super::*;

/// Which `*_over_time` function an [`OverTimeUdf`] evaluates.
///
/// Only the non-experimental, float-typed members that the operator path
/// supports appear here. `mad_over_time`, `first_over_time`, and the
/// `ts_of_*_over_time` family stay on the interpreter.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum OverTimeFamily {
    /// Sum of the window's sample values.
    Sum,
    /// Arithmetic mean of the window's sample values.
    Avg,
    /// Count of samples in the window.
    Count,
    /// Smallest sample value. Prometheus folds NaN out, the same as the engine
    /// and the `prom_min` aggregate.
    Min,
    /// Largest sample value. Prometheus folds NaN out, the same as the engine
    /// and the `prom_max` aggregate.
    Max,
    /// Population standard deviation of the window's sample values.
    Stddev,
    /// Population variance of the window's sample values.
    Stdvar,
    /// Value of the latest (max-timestamp) sample in the window.
    Last,
    /// `1.0` if the window holds any sample.
    Present,
    /// `phi`-quantile of the window's sample values, with linear interpolation.
    /// This matches the engine's `quantile_value`.
    Quantile,
}

impl OverTimeFamily {
    /// Returns the registered UDF name for this family.
    #[must_use]
    pub fn udf_name(self) -> &'static str {
        match self {
            Self::Sum => "prom_sum_over_time",
            Self::Avg => "prom_avg_over_time",
            Self::Count => "prom_count_over_time",
            Self::Min => "prom_min_over_time",
            Self::Max => "prom_max_over_time",
            Self::Stddev => "prom_stddev_over_time",
            Self::Stdvar => "prom_stdvar_over_time",
            Self::Last => "prom_last_over_time",
            Self::Present => "prom_present_over_time",
            Self::Quantile => "prom_quantile_over_time",
        }
    }

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
            Self::Sum => values.iter().sum(),
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
