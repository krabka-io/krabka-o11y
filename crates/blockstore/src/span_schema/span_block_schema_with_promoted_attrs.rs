use super::*;

#[must_use]
pub fn span_block_schema_with_promoted_attrs(promoted_attrs: &[PromotedSpanAttr]) -> SchemaRef {
    let mut fields = vec![
        Field::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false),
        Field::new(SCOL_SPAN_ID, DataType::FixedSizeBinary(8), false),
        Field::new(SCOL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(SCOL_NESTED_SET_LEFT, DataType::Int32, false),
        Field::new(SCOL_NESTED_SET_RIGHT, DataType::Int32, false),
        Field::new(SCOL_PARENT_ID, DataType::Int32, false),
        Field::new(SCOL_CHILD_COUNT, DataType::Int32, false),
        Field::new(SCOL_ROOT_SERVICE_NAME, DataType::Utf8, true),
        Field::new(SCOL_ROOT_SPAN_NAME, DataType::Utf8, true),
        Field::new(SCOL_TRACE_START_NANO, DataType::Int64, false),
        Field::new(SCOL_TRACE_DURATION_NANOS, DataType::Int64, false),
        Field::new(SCOL_NAME, DataType::Utf8, true),
        Field::new(SCOL_KIND, DataType::Int32, false),
        Field::new(SCOL_START_NANO, DataType::Int64, false),
        Field::new(SCOL_DURATION_NANOS, DataType::Int64, false),
        Field::new(SCOL_STATUS_CODE, DataType::Int32, false),
        Field::new(SCOL_STATUS_MESSAGE, DataType::Utf8, true),
        Field::new(SCOL_INSTRUMENTATION_NAME, DataType::Utf8, true),
        Field::new(SCOL_INSTRUMENTATION_VERSION, DataType::Utf8, true),
    ];
    fields.extend(
        promoted_attrs
            .iter()
            .map(|attr| Field::new(attr.column_name(), attr.data_type(), true)),
    );
    fields.extend([
        Field::new(SCOL_ATTR_KEYS, list_of("item", DataType::Utf8, true), true),
        Field::new(
            SCOL_ATTR_IS_ARRAY,
            list_of("item", DataType::Boolean, true),
            true,
        ),
        Field::new(SCOL_ATTR_VALUE, list_list_of(DataType::Utf8), true),
        Field::new(SCOL_ATTR_VALUE_INT, list_list_of(DataType::Int64), true),
        Field::new(
            SCOL_ATTR_VALUE_DOUBLE,
            list_list_of(DataType::Float64),
            true,
        ),
        Field::new(SCOL_ATTR_VALUE_BOOL, list_list_of(DataType::Boolean), true),
        Field::new(SCOL_EVENTS, list_of("item", event_struct(), true), true),
        Field::new(SCOL_LINKS, list_of("item", link_struct(), true), true),
    ]);
    Arc::new(Schema::new(fields))
}
