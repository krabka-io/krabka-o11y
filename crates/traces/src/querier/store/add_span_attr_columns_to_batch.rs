use num_traits::ToPrimitive;

use super::{
    Arc, ArrayRef, AttrValue, BooleanArray, DataType, Field, Float64Array, Int64Array,
    ProjectedAttrColumn, RecordBatch, Schema, StringArray, TraceqlError, attr_values_with_resource,
};

pub(crate) fn add_span_attr_columns_to_batch(
    batch: &RecordBatch,
    wanted: &[ProjectedAttrColumn<DataType>],
) -> Result<RecordBatch, TraceqlError> {
    let schema = batch.schema();
    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    let mut columns = batch.columns().to_vec();
    for ProjectedAttrColumn {
        column_name,
        lookup_key,
        resource,
        data_type,
    } in wanted
    {
        if schema.column_with_name(column_name).is_some() {
            continue; // already a (promoted) column
        }
        let mut values = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let value = attr_values_with_resource(batch, row, *resource)?
                .into_iter()
                .find(|(key, _)| key == lookup_key)
                .map(|(_, value)| value);
            values.push(value);
        }
        // Keep strings and booleans distinct from numbers. Numeric projections
        // may widen integers to floats, but the string "7" is never the number 7.
        let column: ArrayRef = match data_type {
            DataType::Utf8 => Arc::new(StringArray::from(
                values
                    .into_iter()
                    .map(|value| match value {
                        Some(AttrValue::Str(value)) => Some(value),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(
                values
                    .into_iter()
                    .map(|value| match value {
                        Some(AttrValue::Int(value)) => Some(value),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Float64 => Arc::new(Float64Array::from(
                values
                    .into_iter()
                    .map(|value| match value {
                        Some(AttrValue::Float(value)) => Some(value),
                        Some(AttrValue::Int(value)) => value.to_f64(),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DataType::Boolean => Arc::new(BooleanArray::from(
                values
                    .into_iter()
                    .map(|value| match value {
                        Some(AttrValue::Bool(value)) => Some(value),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            _ => {
                return Err(TraceqlError::Store(format!(
                    "unsupported projection type for {column_name}: {data_type:?}"
                )));
            }
        };
        fields.push(Field::new(column_name.clone(), data_type.clone(), true));
        columns.push(column);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|err| TraceqlError::Store(format!("materialize attribute columns: {err}")))
}
