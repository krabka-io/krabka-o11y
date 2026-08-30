use super::*;

pub(crate) fn add_span_attr_columns_to_batch(
    batch: &RecordBatch,
    wanted: &[(String, String, bool)],
) -> Result<RecordBatch, TraceqlError> {
    let schema = batch.schema();
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    let mut columns = batch.columns().to_vec();
    for (column_name, lookup_key, include_resource) in wanted {
        if schema.column_with_name(column_name).is_some() {
            continue; // already a (promoted) column
        }
        let mut values: Vec<Option<String>> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let value = attr_values_with_resource(batch, row, *include_resource)?
                .into_iter()
                .find(|(key, _)| key == lookup_key)
                .map(|(_, value)| attr_value_label(&value));
            values.push(value);
        }
        fields.push(Field::new(column_name.clone(), DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|err| TraceqlError::Store(format!("materialize attribute columns: {err}")))
}
