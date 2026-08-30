use super::*;

pub(crate) fn series_labels_for_kind(
    rows: &TenantCompactionRows,
    kind: MetricBlockKind,
) -> Vec<CompactionSeriesLabels> {
    let fingerprints = match kind {
        MetricBlockKind::Float => rows
            .float_rows
            .iter()
            .map(|row| row.fingerprint)
            .collect::<BTreeSet<_>>(),
        MetricBlockKind::NativeHistograms => rows
            .histogram_rows
            .iter()
            .map(|row| row.fingerprint)
            .collect::<BTreeSet<_>>(),
        MetricBlockKind::Exemplars => rows
            .exemplar_rows
            .iter()
            .map(|row| row.fingerprint)
            .collect::<BTreeSet<_>>(),
        MetricBlockKind::Metadata => rows
            .metadata_rows
            .iter()
            .map(|row| row.fingerprint)
            .collect::<BTreeSet<_>>(),
        MetricBlockKind::ClockReadings => rows
            .clock_rows
            .iter()
            .map(|row| row.fingerprint)
            .collect::<BTreeSet<_>>(),
    };

    fingerprints
        .into_iter()
        .filter_map(|fingerprint| {
            rows.series_labels
                .get(&fingerprint)
                .cloned()
                .map(|labels| CompactionSeriesLabels {
                    fingerprint,
                    labels,
                })
        })
        .collect()
}
