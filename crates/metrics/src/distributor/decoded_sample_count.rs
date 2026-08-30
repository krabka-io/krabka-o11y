use super::DecodedSeries;

pub(crate) fn decoded_sample_count(series: &[DecodedSeries]) -> usize {
    series
        .iter()
        .map(|series| series.samples.len() + series.histograms.len() + series.exemplars.len())
        .sum()
}
