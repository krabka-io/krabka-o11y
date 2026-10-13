use super::{
    BTreeMap, ClockReadingRow, FloatRow, MetadataRow, NativeHistogramRow, SamplePayload,
    TenantCompactionRows, WalRecord, exemplar_row,
};

/// Groups WAL records by tenant and sorts the rows by `(fingerprint, timestamp)`.
#[must_use]
pub fn compact_wal_records(records: &[WalRecord]) -> Vec<TenantCompactionRows> {
    let mut tenants = BTreeMap::<String, TenantCompactionRows>::new();
    for record in records {
        let fingerprint = record.series_fingerprint();
        let rows = tenants
            .entry(record.tenant.clone())
            .or_insert_with(|| TenantCompactionRows {
                tenant: record.tenant.clone(),
                series_labels: BTreeMap::new(),
                float_rows: Vec::new(),
                histogram_rows: Vec::new(),
                exemplar_rows: Vec::new(),
                metadata_rows: Vec::new(),
                clock_rows: Vec::new(),
            });
        rows.series_labels
            .entry(fingerprint)
            .or_insert_with(|| record.labels());

        match &record.payload {
            SamplePayload::Float {
                timestamp_ms,
                value,
                start_timestamp_ms,
            } => rows.float_rows.push(FloatRow {
                fingerprint,
                timestamp_ms: *timestamp_ms,
                value: *value,
                start_timestamp_ms: *start_timestamp_ms,
            }),
            SamplePayload::Hist { timestamp_ms, hist } => {
                rows.histogram_rows.push(NativeHistogramRow {
                    fingerprint,
                    timestamp_ms: *timestamp_ms,
                    hist: hist.clone(),
                });
            }
            SamplePayload::Metadata {
                metric_family_name,
                metric_type,
                help,
                unit,
            } => rows.metadata_rows.push(MetadataRow {
                fingerprint,
                metric_family_name: metric_family_name.clone(),
                metric_type: metric_type.clone(),
                help: help.clone(),
                unit: unit.clone(),
            }),
            SamplePayload::ClockReading(payload) => rows.clock_rows.push(ClockReadingRow {
                fingerprint,
                timestamp_ms: payload.timestamp_ms(),
                reading: (**payload).clone(),
            }),
            SamplePayload::Exemplars => {}
        }

        rows.exemplar_rows.extend(
            record
                .exemplars
                .iter()
                .map(|exemplar| exemplar_row(fingerprint, exemplar)),
        );
    }

    let mut out = tenants.into_values().collect::<Vec<_>>();
    for rows in &mut out {
        rows.float_rows.sort_by(|left, right| {
            left.fingerprint
                .cmp(&right.fingerprint)
                .then_with(|| left.timestamp_ms.cmp(&right.timestamp_ms))
        });
        rows.histogram_rows.sort_by(|left, right| {
            left.fingerprint
                .cmp(&right.fingerprint)
                .then_with(|| left.timestamp_ms.cmp(&right.timestamp_ms))
        });
        rows.exemplar_rows.sort_by(|left, right| {
            left.fingerprint
                .cmp(&right.fingerprint)
                .then_with(|| left.timestamp_ms.cmp(&right.timestamp_ms))
        });
        rows.clock_rows.sort_by(|left, right| {
            left.fingerprint
                .cmp(&right.fingerprint)
                .then_with(|| left.timestamp_ms.cmp(&right.timestamp_ms))
        });
        rows.metadata_rows.sort_by(|left, right| {
            left.fingerprint
                .cmp(&right.fingerprint)
                .then_with(|| left.metric_family_name.cmp(&right.metric_family_name))
                .then_with(|| left.metric_type.cmp(&right.metric_type))
                .then_with(|| left.help.cmp(&right.help))
                .then_with(|| left.unit.cmp(&right.unit))
        });
    }
    out
}
