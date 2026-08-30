use super::{
    Arc, DataType, Field, RANGE_SUFFIX, Schema, SchemaRef, range_array_type};

/// Builds the extended range-vector schema that the module contract describes.
///
/// `input_schema` is the per-series scalar schema. It holds the label columns,
/// the `time_index` `Int64` column, and the `field_column` `Float64` column.
/// The returned schema carries the label columns through, keeps a scalar eval
/// `time_index` column, and adds the `<time_index>_range` and
/// `<field_column>_range` [`RangeArray`] columns.
#[must_use]
pub fn build_extended_range_schema(
    input_schema: &Schema,
    time_index: &str,
    field_column: &str,
) -> SchemaRef {
    let mut fields = Vec::with_capacity(input_schema.fields().len() + 2);

    // 1. Label columns, unchanged and in original order.
    for field in input_schema.fields() {
        let name = field.name();
        if name == time_index || name == field_column {
            continue;
        }
        fields.push(field.clone());
    }

    // 2. Scalar eval-timestamp column (reuses the time-index name).
    fields.push(Arc::new(Field::new(time_index, DataType::Int64, false)));

    // 3. Windowed timestamps RangeArray.
    fields.push(Arc::new(Field::new(
        format!("{time_index}{RANGE_SUFFIX}"),
        range_array_type(DataType::Int64, false),
        false,
    )));

    // 4. Windowed values RangeArray.
    fields.push(Arc::new(Field::new(
        format!("{field_column}{RANGE_SUFFIX}"),
        range_array_type(DataType::Float64, false),
        false,
    )));

    Arc::new(Schema::new(fields))
}
