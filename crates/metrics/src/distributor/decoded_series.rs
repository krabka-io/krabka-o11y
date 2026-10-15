use super::*;

pub(crate) fn decoded_series(
    labels: Vec<krabka_blockstore::LabelPair>,
    sample: Option<DecodedSample>,
) -> DecodedSeries {
    let decoded_labels = krabka_blockstore::Labels::from_pairs(labels);
    DecodedSeries {
        labels: decoded_labels,
        samples: sample.into_iter().collect(),
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata: None,
    }
}

pub(crate) fn decoded_sample_count(series: &[DecodedSeries]) -> usize {
    series
        .iter()
        .map(|series| series.samples.len() + series.histograms.len() + series.exemplars.len())
        .sum()
}
