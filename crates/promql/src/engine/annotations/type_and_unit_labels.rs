use crate::EngineOpts;

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
