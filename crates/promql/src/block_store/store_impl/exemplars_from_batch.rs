use super::{Array, BTreeMap, ExemplarRecord, Float64Array, Int64Array, Labels, MapArray, PromqlError, Result, SeriesFingerprint, StringArray, UInt64Array, append_exemplar_label_map};

pub(crate) fn exemplars_from_batch(
    batch: &arrow::record_batch::RecordBatch,
    series_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<ExemplarRecord>> {
    let fingerprints = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| PromqlError::Store("exemplar fingerprint column has wrong type".into()))?;
    let timestamps = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| PromqlError::Store("exemplar timestamp column has wrong type".into()))?;
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| PromqlError::Store("exemplar value column has wrong type".into()))?;
    let trace_ids = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("exemplar trace_id column has wrong type".into()))?;
    let span_ids = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("exemplar span_id column has wrong type".into()))?;
    let label_maps = batch
        .column(5)
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| PromqlError::Store("exemplar labels column has wrong type".into()))?;

    let mut out = Vec::new();
    for row in 0..batch.num_rows() {
        let fp = fingerprints.value(row);
        let Some(series_labels) = series_by_fp.get(&fp) else {
            continue;
        };
        let ts_ms = timestamps.value(row);
        if ts_ms < start_ms || ts_ms > end_ms {
            continue;
        }
        let mut labels = Labels::new();
        if !trace_ids.is_null(row) {
            labels.insert("trace_id", trace_ids.value(row));
        }
        if !span_ids.is_null(row) {
            labels.insert("span_id", span_ids.value(row));
        }
        append_exemplar_label_map(&mut labels, label_maps, row)?;
        out.push(ExemplarRecord {
            series_labels: series_labels.clone(),
            labels,
            ts_ms,
            value: values.value(row),
        });
    }
    Ok(out)
}
