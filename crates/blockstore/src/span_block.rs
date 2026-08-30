//! Encode in-memory span rows into the flattened span block Arrow schema.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder,
        Int64Builder, ListBuilder, StringBuilder, StringDictionaryBuilder, StructBuilder,
    },
    datatypes::{DataType, Field, Fields, Int32Type},
    record_batch::RecordBatch,
};
use krabka_units::prelude::*;

use crate::{
    error::{BlockStoreError, Result},
    nested_set::NestedSet,
    span_schema::{
        PromotedSpanAttr, PromotedSpanAttrType, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, SpanKind,
        StatusCode, span_block_schema_with_promoted_attrs,
    },
};

#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, ListArray,
        StringArray,
    };

    use super::*;
    use crate::span_schema::{
        PromotedSpanAttr, SCOL_KIND, SCOL_NESTED_SET_LEFT, SCOL_TRACE_ID, span_block_schema,
    };

    fn tid() -> [u8; 16] {
        [1; 16]
    }

    fn sample_row(span: u8, parent: Option<u8>, left: i32) -> SpanRow {
        SpanRow {
            trace_id: tid(),
            span_id: [span; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            nested_set: NestedSet {
                nested_set_left: left,
                nested_set_right: left + 1,
                parent_id: 0,
            },
            child_count: 0,
            root_service_name: Some("checkout".into()),
            root_span_name: Some("POST /pay".into()),
            trace_start_unix_nano: 1_000,
            trace_duration: nanos(500),
            name: Some("db.query".into()),
            kind: SpanKind::Client,
            start_unix_nano: 1_100,
            duration: nanos(50),
            status_code: StatusCode::Error,
            status_message: Some("timeout".into()),
            instrumentation_name: Some("tracer".into()),
            instrumentation_version: None,
            attrs: vec![SpanAttr {
                key: "http.method".into(),
                is_array: false,
                value: AttrValue::Str(vec!["GET".into()]),
            }],
            events: vec![SpanEvent {
                name: "exception".into(),
                time_since_start: nanos(10),
                attrs: vec![("exception.type".into(), "IOError".into())],
            }],
            links: vec![SpanLink {
                linked_trace_id: [2; 16],
                linked_span_id: [3; 8],
                attrs: vec![],
            }],
        }
    }

    /// Every attribute value type reaches its own column, and the array flag
    /// is carried per attribute. The shared fixture gives each row a single
    /// scalar `Int` attribute, so dropping the `Double` or `Bool` arm and
    /// forcing `is_array` to false are all invisible through it.
    #[test]
    fn every_attribute_value_type_reaches_its_own_column() {
        use crate::span_schema::{
            SCOL_ATTR_IS_ARRAY, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, SCOL_ATTR_VALUE_BOOL,
            SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT,
        };

        let mut row = sample_row(1, None, 0);
        row.attrs = vec![
            SpanAttr {
                key: "s".into(),
                is_array: false,
                value: AttrValue::Str(vec!["one".into()]),
            },
            SpanAttr {
                key: "i".into(),
                is_array: false,
                value: AttrValue::Int(vec![7]),
            },
            SpanAttr {
                key: "d".into(),
                is_array: false,
                value: AttrValue::Double(vec![1.5]),
            },
            SpanAttr {
                key: "b".into(),
                is_array: true,
                value: AttrValue::Bool(vec![true, false]),
            },
        ];

        let batch = encode_span_rows(&[row]).expect("encodes");
        let column = |name: &str| {
            let index = batch.schema().index_of(name).expect("a column");
            batch
                .column(index)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("a list column")
                .clone()
        };
        let leaf = |name: &str| {
            column(name)
                .values()
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("a list of lists")
                .values()
                .clone()
        };

        let strings = leaf(SCOL_ATTR_VALUE);
        let strings = strings
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert2::assert!(strings.iter().flatten().collect::<Vec<_>>() == vec!["one"]);

        let ints = leaf(SCOL_ATTR_VALUE_INT);
        let ints = ints.as_any().downcast_ref::<Int64Array>().expect("i64");
        assert2::assert!(ints.iter().flatten().collect::<Vec<_>>() == vec![7_i64]);

        let doubles = leaf(SCOL_ATTR_VALUE_DOUBLE);
        let doubles = doubles
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64");
        assert2::assert!(doubles.len() == 1, "the double attribute keeps its value");
        assert2::assert!((doubles.value(0) - 1.5).abs() < f64::EPSILON);

        let bools = leaf(SCOL_ATTR_VALUE_BOOL);
        let bools = bools.as_any().downcast_ref::<BooleanArray>().expect("bool");
        assert2::assert!(bools.iter().flatten().collect::<Vec<_>>() == vec![true, false]);

        let keys = column(SCOL_ATTR_KEYS);
        let keys = keys
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert2::assert!(keys.iter().flatten().collect::<Vec<_>>() == vec!["s", "i", "d", "b"]);

        // The flag is per attribute, and only the last one is an array.
        let flags = column(SCOL_ATTR_IS_ARRAY);
        let flags = flags
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("bool");
        assert2::assert!(
            flags.iter().flatten().collect::<Vec<_>>() == vec![false, false, false, true]
        );
    }

    #[test]
    fn encode_matches_schema_and_columns() {
        let rows = vec![sample_row(1, None, 1), sample_row(2, Some(1), 2)];
        let batch = encode_span_rows(&rows).unwrap();

        let tids = batch
            .column_by_name(SCOL_TRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        let kinds = batch
            .column_by_name(SCOL_KIND)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let lefts = batch
            .column_by_name(SCOL_NESTED_SET_LEFT)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert2::assert!(batch.schema() == span_block_schema());
        assert2::assert!(batch.num_rows() == 2);
        assert2::assert!(tids.value(0) == [1_u8; 16].as_slice());
        assert2::assert!(kinds.value(0) == SpanKind::Client.as_i32());
        assert2::assert!(lefts.value(1) == 2);
    }

    #[test]
    fn duration_columns_hold_exact_nanosecond_integers() {
        // The block format stores nanoseconds as `Int64`. `SpanRow` carries the
        // durations as `Time` now, so this pins that the encoded columns still
        // hold the exact integers — down to a single nanosecond, and up to a
        // magnitude far past any real span.
        let mut row = sample_row(1, None, 1);
        row.trace_duration = Time::from_nanos(9_007_199_254_740_991);
        row.duration = nanos(1);
        row.events = vec![SpanEvent {
            name: "exception".into(),
            time_since_start: Time::from_nanos(1_234_567_891),
            attrs: vec![],
        }];
        let batch = encode_span_rows(&[row]).unwrap();

        let int64 = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        let events = batch
            .column_by_name(crate::span_schema::SCOL_EVENTS)
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let event_time = events
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap()
            .column_by_name("time_since_start_nano")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        assert2::assert!(
            int64(crate::span_schema::SCOL_TRACE_DURATION_NANOS) == 9_007_199_254_740_991
        );
        assert2::assert!(int64(crate::span_schema::SCOL_DURATION_NANOS) == 1);
        assert2::assert!(event_time == 1_234_567_891);
    }

    fn row_with_attrs(attrs: Vec<SpanAttr>) -> SpanRow {
        let mut row = sample_row(1, None, 1);
        row.attrs = attrs;
        row.events = vec![];
        row.links = vec![];
        row
    }

    #[test]
    fn promotes_double_attr_into_its_column() {
        // Guards the `Some(AttrValue::Double(values))` match arm in
        // PromotedAttrBuilder::append: deleting it falls through to
        // append_null, so the promoted column would be null instead of 1.5.
        let promoted = [PromotedSpanAttr::double("latency")];
        let row = row_with_attrs(vec![SpanAttr {
            key: "latency".into(),
            is_array: false,
            value: AttrValue::Double(vec![1.5]),
        }]);
        let batch = encode_span_rows_with_promoted_attrs(&[row], &promoted).unwrap();
        let col = batch
            .column_by_name(&promoted[0].column_name())
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert2::assert!(!col.is_null(0));
        assert2::assert!((col.value(0) - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn promotes_bool_attr_into_its_column() {
        // Guards the `Some(AttrValue::Bool(values))` match arm.
        let promoted = [PromotedSpanAttr::bool("ok")];
        let row = row_with_attrs(vec![SpanAttr {
            key: "ok".into(),
            is_array: false,
            value: AttrValue::Bool(vec![true]),
        }]);
        let batch = encode_span_rows_with_promoted_attrs(&[row], &promoted).unwrap();
        let col = batch
            .column_by_name(&promoted[0].column_name())
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert2::assert!(!col.is_null(0));
        assert2::assert!(col.value(0));
    }

    #[test]
    fn promotes_string_and_int_attrs_into_their_columns() {
        let promoted = [
            PromotedSpanAttr::string("svc"),
            PromotedSpanAttr::int("code"),
        ];
        let row = row_with_attrs(vec![
            SpanAttr {
                key: "svc".into(),
                is_array: false,
                value: AttrValue::Str(vec!["checkout".into()]),
            },
            SpanAttr {
                key: "code".into(),
                is_array: false,
                value: AttrValue::Int(vec![42]),
            },
        ]);
        let batch = encode_span_rows_with_promoted_attrs(&[row], &promoted).unwrap();

        let svc = batch
            .column_by_name(&promoted[0].column_name())
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
            .unwrap();
        let svc_values = svc.values().as_any().downcast_ref::<StringArray>().unwrap();
        let key = usize::try_from(svc.keys().value(0)).unwrap();

        let code = batch
            .column_by_name(&promoted[1].column_name())
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert2::assert!(svc_values.value(key) == "checkout");
        assert2::assert!(code.value(0) == 42);
    }

    #[test]
    fn generic_attr_lists_carry_keys_and_string_values() {
        // Exercises the str-list and str-list-of-list builders (new_str_list /
        // new_str_list_list) by reading back the generic attr_keys and
        // attr_value columns and asserting their exact contents.
        let row = row_with_attrs(vec![SpanAttr {
            key: "http.method".into(),
            is_array: false,
            value: AttrValue::Str(vec!["GET".into(), "POST".into()]),
        }]);
        let batch = encode_span_rows(&[row]).unwrap();

        let keys = batch
            .column_by_name(crate::span_schema::SCOL_ATTR_KEYS)
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let row0_keys = keys.value(0);
        let row0_keys = row0_keys.as_any().downcast_ref::<StringArray>().unwrap();

        let values = batch
            .column_by_name(crate::span_schema::SCOL_ATTR_VALUE)
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let row0_values = values.value(0);
        let row0_values = row0_values.as_any().downcast_ref::<ListArray>().unwrap();
        let first_attr = row0_values.value(0);
        let first_attr = first_attr.as_any().downcast_ref::<StringArray>().unwrap();
        assert2::assert!(row0_keys.len() == 1);
        assert2::assert!(row0_keys.value(0) == "http.method");
        assert2::assert!(first_attr.len() == 2);
        assert2::assert!(first_attr.value(0) == "GET");
        assert2::assert!(first_attr.value(1) == "POST");
    }
}

// === split-modules: generated submodules ===
mod append_attrs;
mod append_events;
mod append_kv;
mod append_links;
mod attr_value;
mod encode_span_rows;
mod encode_span_rows_with_promoted_attrs;
mod new_event_struct_builder;
mod new_link_struct_builder;
mod new_str_list;
mod new_str_list_list;
mod promoted_attr_builder;
mod promoted_attr_value;
mod span_attr;
mod span_column_builders;
mod span_event;
mod span_link;
mod span_row;

use append_attrs::append_attrs;
use append_events::append_events;
use append_kv::append_kv;
use append_links::append_links;
pub use attr_value::AttrValue;
pub use encode_span_rows::encode_span_rows;
pub use encode_span_rows_with_promoted_attrs::encode_span_rows_with_promoted_attrs;
use new_event_struct_builder::new_event_struct_builder;
use new_link_struct_builder::new_link_struct_builder;
use new_str_list::new_str_list;
use new_str_list_list::new_str_list_list;
use promoted_attr_builder::PromotedAttrBuilder;
use promoted_attr_value::promoted_attr_value;
pub use span_attr::SpanAttr;
use span_column_builders::SpanColumnBuilders;
pub use span_event::SpanEvent;
pub use span_link::SpanLink;
pub use span_row::SpanRow;
