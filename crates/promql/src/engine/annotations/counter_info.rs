use super::emit_info;
use crate::{EngineOpts, PromqlLabels as Labels};

/// Whether series carry Prometheus' `__type__` and `__unit__` labels, which
/// decides how the `metric might not be a counter` info judges a metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TypeAndUnitLabels {
    /// The info reads the `__type__` label.
    Enabled,
    /// The info reads the metric-name suffix.
    Disabled,
}

impl TypeAndUnitLabels {
    /// The setting `opts.enable_type_and_unit_labels` selects.
    pub(crate) fn from_engine_opts(opts: &EngineOpts) -> Self {
        if opts.enable_type_and_unit_labels {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }
}

pub(crate) fn metric_might_not_be_counter_info(metric: &str, metric_type: &str) -> String {
    format!(
        "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got {metric_type:?}: {metric:?}"
    )
}

pub(crate) fn emit_metric_might_not_be_counter_info(
    labels: &Labels,
    type_and_unit_labels: TypeAndUnitLabels,
) {
    let metric = labels.get("__name__").unwrap_or("");
    let metric_type = labels.get("__type__").unwrap_or("");
    if type_and_unit_labels == TypeAndUnitLabels::Disabled {
        if !metric.is_empty()
            && !["_total", "_sum", "_count", "_bucket"]
                .iter()
                .any(|suffix| metric.ends_with(suffix))
        {
            emit_info(format!(
                "PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: {metric:?}"
            ));
        }
        return;
    }
    if !metric.is_empty() && !matches!(metric_type, "counter" | "histogram") {
        emit_info(metric_might_not_be_counter_info(metric, metric_type));
    }
}
