use super::*;

/// Removes the HA coordination label from the series before the WAL append.
pub fn strip_replica_label(series: &mut [DecodedSeries]) {
    for series in series {
        series.labels = series
            .labels
            .iter()
            .filter(|(name, _)| name.as_str() != "__replica__")
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
    }
}
