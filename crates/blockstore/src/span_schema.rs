//! Flattened span-per-row block schema.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

use crate::block_index::{BlockSchema, RequiredColumn};

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn identity_columns_are_fixed_size_binary() {
        let s = span_block_schema();
        for (name, want) in [
            (SCOL_TRACE_ID, DataType::FixedSizeBinary(16)),
            (SCOL_SPAN_ID, DataType::FixedSizeBinary(8)),
            (SCOL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8)),
        ] {
            assert2::assert!(s.column_with_name(name).unwrap().1.data_type() == &want);
        }
    }

    #[test]
    fn nested_set_columns_are_int32() {
        let s = span_block_schema();
        for (_name, column) in [
            ("nested-set left", SCOL_NESTED_SET_LEFT),
            ("nested-set right", SCOL_NESTED_SET_RIGHT),
            ("parent id", SCOL_PARENT_ID),
            ("child count", SCOL_CHILD_COUNT),
        ] {
            assert2::assert!(s.column_with_name(column).unwrap().1.data_type() == &DataType::Int32);
        }
    }

    #[test]
    fn generic_attr_value_is_list_of_utf8() {
        let s = span_block_schema();
        let (_, f) = s.column_with_name(SCOL_ATTR_VALUE).unwrap();
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
        for (_name, column) in [("events", SCOL_EVENTS), ("links", SCOL_LINKS)] {
            let (_, f) = s.column_with_name(column).unwrap();
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
        for (_name, kind) in [
            ("unspecified", SpanKind::Unspecified),
            ("internal", SpanKind::Internal),
            ("server", SpanKind::Server),
            ("client", SpanKind::Client),
            ("producer", SpanKind::Producer),
            ("consumer", SpanKind::Consumer),
        ] {
            assert2::assert!(SpanKind::from_i32(kind.as_i32()) == kind);
        }
        for (_name, status) in [
            ("unset", StatusCode::Unset),
            ("ok", StatusCode::Ok),
            ("error", StatusCode::Error),
        ] {
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
                }
        );
    }
}

mod event_struct;
mod link_struct;
mod list_list_of;
mod list_of;
mod promoted_span_attr;
mod promoted_span_attr_type;
mod scol_attr_is_array;
mod scol_attr_keys;
mod scol_attr_value;
mod scol_attr_value_bool;
mod scol_attr_value_double;
mod scol_attr_value_int;
mod scol_child_count;
mod scol_duration_nanos;
mod scol_events;
mod scol_instrumentation_name;
mod scol_instrumentation_version;
mod scol_kind;
mod scol_links;
mod scol_name;
mod scol_nested_set_left;
mod scol_nested_set_right;
mod scol_parent_id;
mod scol_parent_span_id;
mod scol_promoted_attr_prefix;
mod scol_root_service_name;
mod scol_root_span_name;
mod scol_span_id;
mod scol_start_nano;
mod scol_status_code;
mod scol_status_message;
mod scol_trace_duration_nanos;
mod scol_trace_id;
mod scol_trace_start_nano;
mod span_block_decl;
mod span_block_schema;
mod span_block_schema_with_promoted_attrs;
mod span_kind;
mod status_code;

use event_struct::event_struct;
use link_struct::link_struct;
use list_list_of::list_list_of;
use list_of::list_of;
pub use promoted_span_attr::PromotedSpanAttr;
pub use promoted_span_attr_type::PromotedSpanAttrType;
pub use scol_attr_is_array::SCOL_ATTR_IS_ARRAY;
pub use scol_attr_keys::SCOL_ATTR_KEYS;
pub use scol_attr_value::SCOL_ATTR_VALUE;
pub use scol_attr_value_bool::SCOL_ATTR_VALUE_BOOL;
pub use scol_attr_value_double::SCOL_ATTR_VALUE_DOUBLE;
pub use scol_attr_value_int::SCOL_ATTR_VALUE_INT;
pub use scol_child_count::SCOL_CHILD_COUNT;
pub use scol_duration_nanos::SCOL_DURATION_NANOS;
pub use scol_events::SCOL_EVENTS;
pub use scol_instrumentation_name::SCOL_INSTRUMENTATION_NAME;
pub use scol_instrumentation_version::SCOL_INSTRUMENTATION_VERSION;
pub use scol_kind::SCOL_KIND;
pub use scol_links::SCOL_LINKS;
pub use scol_name::SCOL_NAME;
pub use scol_nested_set_left::SCOL_NESTED_SET_LEFT;
pub use scol_nested_set_right::SCOL_NESTED_SET_RIGHT;
pub use scol_parent_id::SCOL_PARENT_ID;
pub use scol_parent_span_id::SCOL_PARENT_SPAN_ID;
pub use scol_promoted_attr_prefix::SCOL_PROMOTED_ATTR_PREFIX;
pub use scol_root_service_name::SCOL_ROOT_SERVICE_NAME;
pub use scol_root_span_name::SCOL_ROOT_SPAN_NAME;
pub use scol_span_id::SCOL_SPAN_ID;
pub use scol_start_nano::SCOL_START_NANO;
pub use scol_status_code::SCOL_STATUS_CODE;
pub use scol_status_message::SCOL_STATUS_MESSAGE;
pub use scol_trace_duration_nanos::SCOL_TRACE_DURATION_NANOS;
pub use scol_trace_id::SCOL_TRACE_ID;
pub use scol_trace_start_nano::SCOL_TRACE_START_NANO;
pub use span_block_decl::span_block_decl;
pub use span_block_schema::span_block_schema;
pub use span_block_schema_with_promoted_attrs::span_block_schema_with_promoted_attrs;
pub use span_kind::SpanKind;
pub use status_code::StatusCode;
