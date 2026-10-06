use super::DecodedSeries;

pub(crate) fn label_pairs(series: &DecodedSeries) -> Vec<(String, crate::MetricString)> {
    series
        .labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone().into()))
        .collect()
}
