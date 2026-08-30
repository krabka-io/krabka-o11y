use super::DecodedSeries;

pub(crate) fn label_pairs(series: &DecodedSeries) -> Vec<(String, String)> {
    series
        .labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}
