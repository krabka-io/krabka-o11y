use super::{DecodedSeries, SamplePayload, WalExemplar, WalRecord, label_pairs};

/// Fans the decoded series into one WAL record per float sample or native-
/// histogram sample.
#[must_use]
pub fn wal_records_from_series(tenant: &str, series: &[DecodedSeries]) -> Vec<WalRecord> {
    let mut out = Vec::new();
    for series in series {
        let mut labels = label_pairs(series);
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

        let payloads = series
            .samples
            .iter()
            .map(|sample| SamplePayload::Float {
                timestamp_ms: sample.timestamp_ms,
                value: sample.value,
                start_timestamp_ms: sample.start_timestamp_ms,
            })
            .chain(
                series
                    .histograms
                    .iter()
                    .map(|(timestamp_ms, hist)| SamplePayload::Hist {
                        timestamp_ms: *timestamp_ms,
                        hist: hist.clone(),
                    }),
            );
        let sample_count = series.samples.len() + series.histograms.len();
        out.extend(payloads.enumerate().map(|(index, payload)| WalRecord {
            tenant: tenant.to_string(),
            labels: if index + 1 == sample_count && exemplars.is_empty() {
                std::mem::take(&mut labels)
            } else {
                labels.clone()
            },
            payload,
            exemplars: Vec::new(),
        }));
        if let Some(metadata) = &series.metadata {
            // Classic histogram and summary samples carry suffixed names;
            // metadata is indexed by the unsuffixed metric family.
            let mut metadata_labels = series.labels.clone();
            metadata_labels.insert("__name__", metadata.metric_family_name.as_str());
            out.push(WalRecord {
                tenant: tenant.to_string(),
                labels: metadata_labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone().into()))
                    .collect(),
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
