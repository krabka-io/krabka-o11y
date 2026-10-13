use super::DecodedSeries;

/// Removes the HA coordination label from the series before the WAL append.
pub fn strip_replica_label(series: &mut [DecodedSeries]) {
    for series in series {
        let labels = series
            .labels
            .iter()
            .filter(|label| label.name != "__replica__")
            .map(|label| krabka_blockstore::LabelPair::new(label.name, label.value))
            .collect::<Vec<_>>();
        series.labels = krabka_blockstore::Labels::from_pairs(labels);
    }
}
