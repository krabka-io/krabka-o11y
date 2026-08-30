use super::*;

pub(crate) fn extrapolated_histogram_counts(
    extrapolation: &HistogramExtrapolation<'_>,
    histograms: &[NativeHistogram],
    counts: impl Fn(&NativeHistogram) -> &[f64],
) -> Option<Vec<f64>> {
    let bucket_count = counts(histograms.first()?).len();
    let mut out = Vec::with_capacity(bucket_count);
    for index in 0..bucket_count {
        let values = histograms
            .iter()
            .map(|histogram| counts(histogram).get(index).copied())
            .collect::<Option<Vec<_>>>()?;
        out.push(extrapolated_histogram_component(extrapolation, &values)?);
    }
    Some(out)
}
