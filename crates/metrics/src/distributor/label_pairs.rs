use super::*;

pub(crate) fn label_pairs(series: &DecodedSeries) -> Vec<(String, String)> {
    series
        .labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}
