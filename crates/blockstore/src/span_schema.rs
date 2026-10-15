//! Flattened span-per-row block schema.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

use crate::block_index::{BlockSchema, RequiredColumn};

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    struct ColumnCase {
        column: &'static str,
    }

    struct IdentityColumnCase {
        name: &'static str,
        want: DataType,
    }

    #[test]
    fn identity_columns_are_fixed_size_binary() {
        let s = span_block_schema();
        for case in [
            IdentityColumnCase {
                name: SCOL_TRACE_ID,
                want: DataType::FixedSizeBinary(16),
            },
            IdentityColumnCase {
                name: SCOL_SPAN_ID,
                want: DataType::FixedSizeBinary(8),
            },
            IdentityColumnCase {
                name: SCOL_PARENT_SPAN_ID,
                want: DataType::FixedSizeBinary(8),
            },
        ] {
            assert2::assert!(s.column_with_name(case.name).unwrap().1.data_type() == &case.want);
        }
    }

    #[test]
    fn nested_set_columns_are_int32() {
        let s = span_block_schema();
        for case in [
            ColumnCase {
                column: SCOL_NESTED_SET_LEFT,
            },
            ColumnCase {
                column: SCOL_NESTED_SET_RIGHT,
            },
            ColumnCase {
                column: SCOL_PARENT_ID,
            },
            ColumnCase {
                column: SCOL_CHILD_COUNT,
            },
        ] {
            assert2::assert!(
                s.column_with_name(case.column).unwrap().1.data_type() == &DataType::Int32
            );
        }
    }

    #[test]
    fn generic_attr_value_is_list_of_utf8() {
        let s = span_block_schema();
        let f = s.column_with_name(SCOL_ATTR_VALUE).unwrap().1;
        match f.data_type() {
            DataType::List(inner) => match inner.data_type() {
                DataType::List(scalar) => assert2::assert!(scalar.data_type() == &DataType::Utf8),
                other => panic!("expected List<List<Utf8>>, inner {other:?}"),
            },
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn events_and_links_are_list_of_struct() {
        let s = span_block_schema();
        for case in [
            ColumnCase {
                column: SCOL_EVENTS,
            },
            ColumnCase { column: SCOL_LINKS },
        ] {
            let f = s.column_with_name(case.column).unwrap().1;
            match f.data_type() {
                DataType::List(inner) => {
                    assert2::assert!(matches!(inner.data_type(), DataType::Struct(_)));
                }
                other => panic!("expected List<Struct>, got {other:?}"),
            }
        }
    }

    #[test]
    fn kind_and_status_enums_round_trip_i32() {
        for value in 0..=5 {
            let kind = SpanKind::from_i32(value);
            assert2::assert!(SpanKind::from_i32(kind.as_i32()) == kind);
        }
        for status in [StatusCode::Unset, StatusCode::Ok, StatusCode::Error] {
            assert2::assert!(StatusCode::from_i32(status.as_i32()) == status);
        }
    }

    #[test]
    fn span_decl_sort_key_is_trace_id_then_start() {
        assert2::assert!(
            span_block_decl()
                == BlockSchema {
                    required: vec![
                        RequiredColumn::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false,),
                        RequiredColumn::new(SCOL_START_NANO, DataType::Int64, false),
                    ],
                    sort_key: vec![SCOL_TRACE_ID.to_string(), SCOL_START_NANO.to_string()],
                    bloom_columns: vec![SCOL_SPAN_ID.to_string()],
                }
        );
    }
}
mod promoted_span_attr;
mod promoted_span_attr_type;
mod span_kind;
mod status_code;

fn nested_attribute_fields() -> Vec<Field> {
    vec![
        Field::new(SCOL_ATTR_KEYS, list_of("item", DataType::Utf8, true), true),
        Field::new(SCOL_ATTR_VALUE, list_list_of(DataType::Utf8), true),
        Field::new("attr_typed", DataType::Utf8, true),
    ]
}
pub use promoted_span_attr::PromotedSpanAttr;
pub use promoted_span_attr_type::PromotedSpanAttrType;
pub use span_kind::SpanKind;
pub use status_code::StatusCode;

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
        Field::new(SCOL_ATTR_VALUE_DOUBLE, list_list_of(DataType::Float64), true),
        Field::new(SCOL_ATTR_VALUE_BOOL, list_list_of(DataType::Boolean), true),
        Field::new(
            "attr_value_unsupported",
            list_of("item", DataType::Utf8, true),
            true,
        ),
        Field::new(SCOL_EVENTS, list_of("item", event_struct(), true), true),
        Field::new(SCOL_LINKS, list_of("item", link_struct(), true), true),
    ]);
    Arc::new(Schema::new(fields))
}

pub const SCOL_ATTR_IS_ARRAY: &str = "attr_is_array";

pub const SCOL_ATTR_KEYS: &str = "attr_keys";

pub const SCOL_ATTR_VALUE: &str = "attr_value";

pub const SCOL_ATTR_VALUE_BOOL: &str = "attr_value_bool";

pub const SCOL_ATTR_VALUE_DOUBLE: &str = "attr_value_double";

pub const SCOL_ATTR_VALUE_INT: &str = "attr_value_int";

pub const SCOL_CHILD_COUNT: &str = "child_count";

pub const SCOL_DURATION_NANOS: &str = "duration_nanos";

pub const SCOL_EVENTS: &str = "events";

pub const SCOL_INSTRUMENTATION_NAME: &str = "instrumentation_name";

pub const SCOL_INSTRUMENTATION_VERSION: &str = "instrumentation_version";

pub const SCOL_KIND: &str = "kind";

pub const SCOL_LINKS: &str = "links";

pub const SCOL_NAME: &str = "name";

pub const SCOL_NESTED_SET_LEFT: &str = "nested_set_left";

pub const SCOL_NESTED_SET_RIGHT: &str = "nested_set_right";

pub const SCOL_PARENT_ID: &str = "parent_id";

pub const SCOL_PARENT_SPAN_ID: &str = "parent_span_id";

pub const SCOL_PROMOTED_ATTR_PREFIX: &str = "attr.";

pub const SCOL_ROOT_SERVICE_NAME: &str = "root_service_name";

pub const SCOL_ROOT_SPAN_NAME: &str = "root_span_name";

pub const SCOL_SPAN_ID: &str = "span_id";

pub const SCOL_START_NANO: &str = "start_unix_nano";

pub const SCOL_STATUS_CODE: &str = "status_code";

pub const SCOL_STATUS_MESSAGE: &str = "status_message";

pub const SCOL_TRACE_DURATION_NANOS: &str = "trace_duration_nanos";

pub const SCOL_TRACE_ID: &str = "trace_id";

pub const SCOL_TRACE_START_NANO: &str = "trace_start_unix_nano";

pub(crate) fn event_struct() -> DataType {
    let mut fields = vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("time_since_start_nano", DataType::Int64, true),
    ];
    fields.extend(nested_attribute_fields());
    DataType::Struct(Fields::from(fields))
}

pub(crate) fn link_struct() -> DataType {
    let mut fields = vec![
        Field::new("linked_trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("linked_span_id", DataType::FixedSizeBinary(8), true),
    ];
    fields.extend(nested_attribute_fields());
    DataType::Struct(Fields::from(fields))
}

pub(crate) fn list_list_of(inner: DataType) -> DataType {
    list_of("item", list_of("item", inner, true), true)
}

pub(crate) fn list_of(name: &str, inner: DataType, nullable: bool) -> DataType {
    DataType::List(Arc::new(Field::new(name, inner, nullable)))
}

/// The flattened span-per-row Arrow schema.
#[must_use]
pub fn span_block_schema() -> SchemaRef {
    span_block_schema_with_promoted_attrs(&[])
}

/// Span block declaration used by generic schema validation.
#[must_use]
pub fn span_block_decl() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            RequiredColumn::new(SCOL_START_NANO, DataType::Int64, false),
        ],
        sort_key: vec![SCOL_TRACE_ID.to_string(), SCOL_START_NANO.to_string()],
        // The span id, and only the span id. The trace id leads the sort key,
        // so min/max prunes it and the traces index carries its own trace-id
        // bloom besides; but span ids are random bytes scattered through
        // every row group, and an equality on one has nothing else to go on.
        bloom_columns: vec![SCOL_SPAN_ID.to_string()],
    }
}
