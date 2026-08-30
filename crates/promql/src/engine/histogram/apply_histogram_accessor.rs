use super::{HistogramAccessor, InstantSample, SampleValue, labels_without_metric_name};

/// Shared `histogram_quantile(phi, v)` core over an already-evaluated instant
/// vector.
///
/// Backs both the interpreter (`PromqlEngine::eval_histogram_quantile_call`)
/// and the recursive operator path (a `PlannedInstant::Precomputed` result), so
/// the two are identical by construction. Native-histogram samples are reduced
/// via [`native_histogram_quantile`]; classic `<metric>_bucket{le}` float series
/// are grouped by their labels (excluding `le`), folded by
/// [`classic_histogram_quantile`] (which forces bucket monotonicity, parses each
/// `le` bound incl. `+Inf`, handles `<2`-bucket / `phi` out of `[0, 1]` / the
/// negative-first-bucket lower bound, and linearly interpolates). A series whose
/// labelset (sans `le`) appears as both a native histogram and a classic bucket
/// group is dropped from the output with a mixed-schema warning, matching
/// Apply a native-histogram accessor (`histogram_count` / `sum` / `avg` /
/// `stddev` / `stdvar`) to an instant vector, mirroring
/// `PromqlEngine::eval_histogram_accessor_call` exactly.
///
/// Only `SampleValue::Histogram` rows are kept (a float row carries no histogram
/// to read, so it is dropped); each surviving row keeps its source timestamp,
/// drops `__name__`, and carries the scalar accessor value. Shared by the
/// interpreter and the operator path so the two are parity-exact.
pub(crate) fn apply_histogram_accessor(
    samples: Vec<InstantSample>,
    accessor: HistogramAccessor,
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .filter_map(|sample| {
            let SampleValue::Histogram(hist) = sample.value else {
                return None;
            };
            Some(InstantSample {
                labels: labels_without_metric_name(&sample.labels),
                ts_ms: sample.ts_ms,
                value: SampleValue::Float(accessor.value(&hist)),
            })
        })
        .collect()
}
