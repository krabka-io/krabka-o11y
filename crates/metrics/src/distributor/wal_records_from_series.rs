use super::{DecodedSeries, WalRecord, label_pairs, WalExemplar, SamplePayload};

/// Fans the decoded series into one WAL record per float sample or native-
/// histogram sample.
#[must_use]
pub fn wal_records_from_series(tenant: &str, series: &[DecodedSeries]) -> Vec<WalRecord> {
    let mut out = Vec::new();
    for series in series {
        let labels = label_pairs(series);
        let exemplars = series
            .exemplars
            .iter()
            .map(|exemplar| WalExemplar {
                labels: exemplar
                    .labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
                value: exemplar.value,
                timestamp_ms: exemplar.timestamp_ms,
            })
            .collect::<Vec<_>>();

        out.extend(series.samples.iter().map(|sample| WalRecord {
            tenant: tenant.to_string(),
            labels: labels.clone(),
            payload: SamplePayload::Float {
                timestamp_ms: sample.timestamp_ms,
                value: sample.value,
                start_timestamp_ms: sample.start_timestamp_ms,
            },
            exemplars: Vec::new(),
        }));
        out.extend(
            series
                .histograms
                .iter()
                .map(|(timestamp_ms, hist)| WalRecord {
                    tenant: tenant.to_string(),
                    labels: labels.clone(),
                    payload: SamplePayload::Hist {
                        timestamp_ms: *timestamp_ms,
                        hist: hist.clone(),
                    },
                    exemplars: Vec::new(),
                }),
        );
        if let Some(metadata) = &series.metadata {
            out.push(WalRecord {
                tenant: tenant.to_string(),
                labels: labels.clone(),
                payload: SamplePayload::Metadata {
                    metric_family_name: metadata.metric_family_name.clone(),
                    metric_type: metadata.metric_type.clone(),
                    help: metadata.help.clone(),
                    unit: metadata.unit.clone(),
                },
                exemplars: Vec::new(),
            });
        }
        if !exemplars.is_empty() {
            out.push(WalRecord {
                tenant: tenant.to_string(),
                labels,
                payload: SamplePayload::Exemplars,
                exemplars,
            });
        }
    }
    out
}
