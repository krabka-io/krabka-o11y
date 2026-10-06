use super::{
    DataType, ExemplarRow, Field, Float64Builder, HistogramCodecError, Int64Builder, MapBuilder,
    RecordBatch, StringBuilder, UInt64Builder, exemplar_schema,
};

pub(crate) fn encode_exemplar_rows(
    rows: &[ExemplarRow],
) -> Result<RecordBatch, HistogramCodecError> {
    let mut columns = ExemplarColumns::new();

    for row in rows {
        columns.fingerprints.append_value(row.fingerprint);
        columns.timestamps.append_value(row.timestamp_ms);
        columns.values.append_value(row.value);
        columns.trace_ids.append_option(row.trace_id.as_deref());
        columns.span_ids.append_option(row.span_id.as_deref());
        for (name, value) in &row.labels {
            columns.labels.keys().append_value(name);
            columns.labels.values().append_value(value);
        }
        columns.labels.append(true)?;
    }

    Ok(RecordBatch::try_new(exemplar_schema(), columns.finish())?)
}

#[derive(krabka_column_macros::ColumnBuilders)]
struct ExemplarColumns {
    fingerprints: UInt64Builder,
    timestamps: Int64Builder,
    values: Float64Builder,
    trace_ids: StringBuilder,
    span_ids: StringBuilder,
    #[column(init = "new_label_builder()")]
    labels: MapBuilder<StringBuilder, StringBuilder>,
}

fn new_label_builder() -> MapBuilder<StringBuilder, StringBuilder> {
    MapBuilder::new(
        Some(arrow::array::builder::MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_values_field(Field::new("value", DataType::Utf8, false))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float64Array, Int64Array, MapArray, StringArray, UInt64Array};

    use super::*;

    #[test]
    fn keeps_exemplar_values_nullable_ids_and_label_rows() {
        let rows = vec![
            ExemplarRow {
                fingerprint: 17,
                timestamp_ms: -9,
                value: 2.5,
                trace_id: Some("trace-a".into()),
                span_id: None,
                labels: vec![
                    ("kind".into(), "slow".into()),
                    ("region".into(), "west".into()),
                ],
            },
            ExemplarRow {
                fingerprint: 29,
                timestamp_ms: 31,
                value: -4.0,
                trace_id: None,
                span_id: Some("span-b".into()),
                labels: Vec::new(),
            },
        ];
        let batch = encode_exemplar_rows(&rows).unwrap();
        assert2::assert!(batch.schema() == exemplar_schema());
        let fingerprints = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let trace_ids = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let span_ids = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let labels = batch.column(5).as_any().downcast_ref::<MapArray>().unwrap();
        let decoded = (0..batch.num_rows())
            .map(|index| {
                let entries = labels.value(index);
                let names = entries
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let label_values = entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                ExemplarRow {
                    fingerprint: fingerprints.value(index),
                    timestamp_ms: timestamps.value(index),
                    value: values.value(index),
                    trace_id: (!trace_ids.is_null(index))
                        .then(|| trace_ids.value(index).to_owned()),
                    span_id: (!span_ids.is_null(index)).then(|| span_ids.value(index).to_owned()),
                    labels: names
                        .iter()
                        .zip(label_values.iter())
                        .map(|(name, value)| (name.unwrap().to_owned(), value.unwrap().to_owned()))
                        .collect(),
                }
            })
            .collect::<Vec<_>>();
        assert2::assert!(decoded == rows);
    }

    #[test]
    fn empty_exemplars_keep_the_map_schema() {
        let batch = encode_exemplar_rows(&[]).unwrap();
        assert2::assert!(batch.num_rows() == 0);
        assert2::assert!(batch.schema() == exemplar_schema());
    }
}
