use super::{
    HistogramCodecError, Int64Builder, MetadataRow, RecordBatch, StringBuilder, UInt64Builder,
    metadata_schema,
};

pub(crate) fn encode_metadata_rows(
    rows: &[MetadataRow],
) -> Result<RecordBatch, HistogramCodecError> {
    let mut columns = MetadataColumns::new();

    for row in rows {
        columns.fingerprints.append_value(row.fingerprint);
        columns.timestamps.append_value(0);
        columns.names.append_value(&row.metric_family_name);
        columns.types.append_value(&row.metric_type);
        columns.helps.append_value(&row.help);
        columns.units.append_value(&row.unit);
    }

    Ok(RecordBatch::try_new(metadata_schema(), columns.finish())?)
}

#[derive(krabka_column_macros::ColumnBuilders)]
struct MetadataColumns {
    fingerprints: UInt64Builder,
    timestamps: Int64Builder,
    names: StringBuilder,
    types: StringBuilder,
    helps: StringBuilder,
    units: StringBuilder,
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray, UInt64Array};

    use super::*;

    #[test]
    fn keeps_each_metadata_field_and_zero_timestamps() {
        let rows = vec![
            MetadataRow {
                fingerprint: 7,
                metric_family_name: "requests_total".into(),
                metric_type: "counter".into(),
                help: "Request count".into(),
                unit: "requests".into(),
            },
            MetadataRow {
                fingerprint: 11,
                metric_family_name: "temperature".into(),
                metric_type: "gauge".into(),
                help: String::new(),
                unit: "celsius".into(),
            },
        ];
        let batch = encode_metadata_rows(&rows).unwrap();
        assert2::assert!(batch.schema() == metadata_schema());
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
        assert2::assert!(timestamps.iter().collect::<Vec<_>>() == vec![Some(0), Some(0)]);
        let text = |column: usize, index: usize| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(index)
                .to_owned()
        };
        let decoded = (0..batch.num_rows())
            .map(|index| MetadataRow {
                fingerprint: fingerprints.value(index),
                metric_family_name: text(2, index),
                metric_type: text(3, index),
                help: text(4, index),
                unit: text(5, index),
            })
            .collect::<Vec<_>>();
        assert2::assert!(decoded == rows);
        let empty = encode_metadata_rows(&[]).unwrap();
        assert2::assert!(empty.num_rows() == 0);
        assert2::assert!(empty.schema() == metadata_schema());
    }
}
