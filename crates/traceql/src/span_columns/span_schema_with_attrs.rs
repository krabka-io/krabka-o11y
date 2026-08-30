use super::*;

#[must_use]
pub fn span_schema_with_attrs(attr_cols: &[(String, DataType)]) -> SchemaRef {
    let mut fields = vec![
        Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
        Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
        Field::new(COL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(COL_NS_LEFT, DataType::Int32, false),
        Field::new(COL_NS_RIGHT, DataType::Int32, false),
        Field::new(COL_PARENT_ID, DataType::Int32, false),
        Field::new(COL_CHILD_COUNT, DataType::Int32, false),
        Field::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, true),
        Field::new(COL_ROOT_SPAN_NAME, DataType::Utf8, true),
        Field::new(COL_TRACE_START, DataType::Int64, false),
        Field::new(COL_TRACE_DURATION, DataType::Int64, false),
        Field::new(COL_NAME, DataType::Utf8, true),
        Field::new(COL_KIND, DataType::Int32, false),
        Field::new(COL_START, DataType::Int64, false),
        Field::new(COL_DURATION, DataType::Int64, false),
        Field::new(COL_STATUS_CODE, DataType::Int32, false),
        Field::new(COL_STATUS_MESSAGE, DataType::Utf8, true),
        Field::new(COL_INSTRUMENTATION_NAME, DataType::Utf8, true),
        Field::new(COL_INSTRUMENTATION_VERSION, DataType::Utf8, true),
        Field::new(COL_EVENT_NAME, DataType::Utf8, true),
        Field::new(COL_EVENT_TIME_SINCE_START, DataType::Int64, true),
        Field::new(COL_LINK_TRACE_ID, DataType::FixedSizeBinary(16), true),
        Field::new(COL_LINK_SPAN_ID, DataType::FixedSizeBinary(8), true),
    ];

    fields.extend(
        attr_cols
            .iter()
            .map(|(key, dt)| Field::new(format!("{ATTR_PREFIX}{key}"), dt.clone(), true)),
    );

    Arc::new(Schema::new(fields))
}
