use super::{
    Arc, ArrayRef, DataType, ExemplarRow, Field, HistogramCodecError, MapBuilder, RecordBatch,
    StringBuilder, exemplar_schema,
};
use crate::numeric_sample_columns::encode_numeric_sample_columns;

/// Encodes exemplar rows into one record batch of [`exemplar_schema`].
///
/// # Errors
///
/// Returns an error when Arrow rejects the label map or the assembled batch.
pub fn encode_exemplar_rows(rows: &[ExemplarRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut trace_ids = StringBuilder::new();
    let mut span_ids = StringBuilder::new();
    let mut labels = MapBuilder::new(
        Some(arrow::array::builder::MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_values_field(Field::new("value", DataType::Utf8, false));

    for row in rows {
        match &row.trace_id {
            Some(trace_id) => trace_ids.append_value(trace_id),
            None => trace_ids.append_null(),
        }
        match &row.span_id {
            Some(span_id) => span_ids.append_value(span_id),
            None => span_ids.append_null(),
        }
        for pair in &row.labels {
            labels.keys().append_value(&pair.name);
            labels.values().append_value(&pair.value);
        }
        labels.append(true)?;
    }

    let mut columns: Vec<ArrayRef> = Vec::from(encode_numeric_sample_columns(rows));
    columns.push(Arc::new(trace_ids.finish()));
    columns.push(Arc::new(span_ids.finish()));
    columns.push(Arc::new(labels.finish()));

    Ok(RecordBatch::try_new(exemplar_schema(), columns)?)
}
