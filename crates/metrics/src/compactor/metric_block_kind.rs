use super::{Deserialize, Serialize};

/// Metric block payload kind used in deterministic object keys.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MetricBlockKind {
    Float,
    NativeHistograms,
    Exemplars,
    Metadata,
    ClockReadings,
}

impl MetricBlockKind {
    pub(crate) const fn object_path(self) -> &'static str {
        match self {
            Self::Float => "float",
            Self::NativeHistograms => "native-histograms",
            Self::Exemplars => "exemplars",
            Self::Metadata => "metadata",
            Self::ClockReadings => "clock-readings",
        }
    }
}
