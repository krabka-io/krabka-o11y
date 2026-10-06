use super::{ANNOTATIONS, maybe_add_metric_name, with_source_position};
use crate::result::HistogramQuantileRepair;

/// Exact Prometheus `HistogramQuantileForcedMonotonicityInfo` text for `metric`.
///
/// Prometheus leaves the metric name off when it does not know it.
pub(crate) fn emit_histogram_quantile_forced_monotonicity_info(
    metric: &str,
    time_ms: i64,
    repairs: [f64; 3],
) {
    let message = with_source_position(maybe_add_metric_name("PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile)".to_string(), metric));
    let repair = HistogramQuantileRepair::new(repairs[0], repairs[1], repairs[2], time_ms);
    let _ =
        ANNOTATIONS.try_with(|sink| sink.borrow_mut().histogram_quantile_repair(message, repair));
}
