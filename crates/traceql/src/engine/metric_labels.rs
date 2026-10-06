use super::{
    Array, AttrValue, BTreeMap, DataType, Field, Intrinsic, RecordBatch, Result, Scope,
    TraceMetricLabelType, UnixNano, compare_row, compare_row_attr_values, i32_value, i64_value,
    kind_enum_name, metric_duration_label, metric_field_column, metric_label_key,
    metric_label_value, status_enum_name,
};

/// Keep the display labels and scalar types together in the grouping key.
/// Without the type component, integer 1 and string "1" share a bucket.
pub(crate) type MetricLabels = (
    Vec<(String, String)>,
    BTreeMap<String, TraceMetricLabelType>,
);

pub(crate) fn metric_labels(
    batch: &RecordBatch,
    row: usize,
    fields: &[Field],
) -> Result<MetricLabels> {
    let mut labels = Vec::new();
    let mut types = BTreeMap::new();
    let values = compare_row(batch, row, UnixNano(0))?;
    for field in fields {
        let key = metric_label_key(field);
        let raw = compare_row_attr_values(&values, &field.scope, &field.key);
        // Packed fields preserve original types even when promoted columns
        // stringify heterogeneous values. Scopeless lookup prefers the span.
        if let Some(value) = raw.first() {
            let (value, kind) = match value {
                AttrValue::Str(value) => (value.clone(), None),
                AttrValue::Int(value) => (value.to_string(), Some(TraceMetricLabelType::Int)),
                AttrValue::Float(value) => (value.to_string(), Some(TraceMetricLabelType::Double)),
                AttrValue::Bool(value) => (value.to_string(), Some(TraceMetricLabelType::Bool)),
            };
            if let Some(kind) = kind {
                types.insert(key.clone(), kind);
            }
            labels.push((key, value));
            continue;
        }
        let column = metric_field_column(field)?;
        let Some(array) = batch.column_by_name(&column) else {
            continue;
        };
        if array.is_null(row) {
            continue;
        }
        let value = match field.scope {
            Scope::Intrinsic(Intrinsic::Status) => {
                status_enum_name(i32_value(batch, &column, row)?).to_string()
            }
            Scope::Intrinsic(Intrinsic::Kind) => {
                kind_enum_name(i32_value(batch, &column, row)?).to_string()
            }
            Scope::Intrinsic(
                Intrinsic::Duration | Intrinsic::TraceDuration | Intrinsic::EventTimeSinceStart,
            ) => metric_duration_label(i64_value(batch, &column, row)?),
            _ => metric_label_value(batch, &column, row)?,
        };
        let kind = match field.scope {
            Scope::Intrinsic(
                Intrinsic::Status
                | Intrinsic::Kind
                | Intrinsic::Duration
                | Intrinsic::TraceDuration
                | Intrinsic::EventTimeSinceStart,
            ) => None,
            _ => match array.data_type() {
                DataType::Int64 | DataType::Int32 => Some(TraceMetricLabelType::Int),
                DataType::Float64 => Some(TraceMetricLabelType::Double),
                DataType::Boolean => Some(TraceMetricLabelType::Bool),
                _ => None,
            },
        };
        if let Some(kind) = kind {
            types.insert(key.clone(), kind);
        }
        labels.push((key, value));
    }
    Ok((labels, types))
}
