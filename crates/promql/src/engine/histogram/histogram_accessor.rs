use super::{NativeHistogram, native_histogram_stdvar};

#[derive(Clone, Copy)]
pub(crate) enum HistogramAccessor {
    Count,
    Sum,
    Avg,
    Stddev,
    Stdvar,
}

impl HistogramAccessor {
    pub(crate) fn value(self, hist: &NativeHistogram) -> f64 {
        match self {
            Self::Count => hist.count,
            Self::Sum => hist.sum,
            Self::Avg => hist.sum / hist.count,
            Self::Stddev => native_histogram_stdvar(hist).sqrt(),
            Self::Stdvar => native_histogram_stdvar(hist),
        }
    }
}
