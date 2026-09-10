use super::{NativeHistogram, ResetHint, histogram_chunk_appendable};

/// Rewrites the counter-reset hints of one loaded series the way Prometheus's
/// TSDB reports them on the way back out.
///
/// A `.test` file writes the hint it wants a sample *stored* with, but a query
/// never sees that hint. Prometheus reads samples out of chunks, and
/// `chunkenc.counterResetHint` derives the hint from the chunk instead: gauge
/// for a gauge chunk, `not_reset` for every sample after the first in a counter
/// chunk, and `unknown` for the first sample in one, because the chunk before
/// it may since have been dropped. Chunks are cut on a counter reset, on a
/// layout change, and where a series moves between gauge and counter
/// histograms or between floats and histograms.
///
/// Without this the corpus disagrees with upstream wherever an expectation
/// carries a hint, which is why `range_queries.test` expects `not_reset` from a
/// series loaded as plain `{{count:0}}+{{count:1}}x4`.
#[derive(Debug, Default)]
pub(crate) struct ChunkResetHints {
    /// Last histogram in the open histogram chunk, if a histogram chunk is open.
    last: Option<NativeHistogram>,
    /// Whether the open chunk is a gauge chunk.
    gauge: bool,
    /// Samples already written to the open chunk.
    samples: usize,
}

impl ChunkResetHints {
    /// Records a float sample, which closes any open histogram chunk.
    pub(crate) fn push_float(&mut self) {
        self.last = None;
        self.samples = 0;
    }

    /// Records a histogram sample and returns the hint a query reads back.
    pub(crate) fn push_histogram(&mut self, histogram: &NativeHistogram) -> ResetHint {
        let gauge = histogram.reset_hint == ResetHint::Gauge;
        let appendable = self.gauge == gauge
            && self
                .last
                .as_ref()
                .is_some_and(|last| histogram_chunk_appendable(last, histogram));
        if appendable {
            self.samples += 1;
        } else {
            self.gauge = gauge;
            self.samples = 1;
        }
        self.last = Some(histogram.clone());
        if gauge {
            ResetHint::Gauge
        } else if self.samples > 1 {
            ResetHint::No
        } else {
            ResetHint::Unknown
        }
    }
}
