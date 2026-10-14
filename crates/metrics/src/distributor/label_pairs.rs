use super::DecodedSeries;

pub(crate) fn label_pairs(
    series: &DecodedSeries,
) -> Vec<crate::MetricLabelPair<crate::MetricString>> {
    series
        .labels
        .iter()
        .map(|pair| crate::MetricLabelPair::new(pair.name, pair.value.into()))
        .collect()
}
