//! Encode in-memory span rows into the flattened span block Arrow schema.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder,
        Int32Builder, Int64Builder, ListBuilder, StringBuilder, StringDictionaryBuilder,
        StructBuilder,
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
                attrs: vec![SpanAttr {
                    key: "exception.type".into(),
                    is_array: false,
                    value: AttrValue::Str(vec!["IOError".into()]),
                }],
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

    fn encode_promoted_attr(
        key: &str,
        value: AttrValue,
        promoted: &PromotedSpanAttr,
    ) -> RecordBatch {
        let row = row_with_attrs(vec![SpanAttr {
            key: key.into(),
            is_array: false,
            value,
        }]);
        encode_span_rows_with_promoted_attrs(&[row], std::slice::from_ref(promoted)).unwrap()
    }

    #[test]
    fn promotes_double_attr_into_its_column() {
        // Guards the `Some(AttrValue::Double(values))` match arm in
        // PromotedAttrBuilder::append: deleting it falls through to
        // append_null, so the promoted column would be null instead of 1.5.
        let promoted = PromotedSpanAttr::double("latency");
        let batch = encode_promoted_attr("latency", AttrValue::Double(vec![1.5]), &promoted);
        let col = batch
            .column_by_name(&promoted.column_name())
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
        let promoted = PromotedSpanAttr::bool("ok");
        let batch = encode_promoted_attr("ok", AttrValue::Bool(vec![true]), &promoted);
        let col = batch
            .column_by_name(&promoted.column_name())
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

mod attr_value;
mod promoted_attr_builder;
mod span_attr;
mod span_column_builders;

pub use attr_value::AttrValue;
use promoted_attr_builder::PromotedAttrBuilder;
pub use span_attr::SpanAttr;
pub use span_column_builders::SpanColumnBuilders;

/// One nested span event.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanEvent {
    pub name: String,
    /// Offset from the owning span's start.
    pub time_since_start: Time,
    pub attrs: Vec<SpanAttr>,
}

/// One nested span link.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanLink {
    /// Trace and span identifiers of the linked span.
    pub linked_trace_id: [u8; 16],
    pub linked_span_id: [u8; 8],
    pub attrs: Vec<SpanAttr>,
}

/// One flattened span row.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub nested_set: NestedSet,
    pub child_count: i32,
    pub root_service_name: Option<String>,
    pub root_span_name: Option<String>,
    pub trace_start_unix_nano: i64,
    pub trace_duration: Time,
    pub name: Option<String>,
    pub kind: SpanKind,
    pub start_unix_nano: i64,
    pub duration: Time,
    pub status_code: StatusCode,
    pub status_message: Option<String>,
    pub instrumentation_name: Option<String>,
    pub instrumentation_version: Option<String>,
    /// Attributes, events and links retained in the flattened row.
    pub attrs: Vec<SpanAttr>,
    pub events: Vec<SpanEvent>,
    pub links: Vec<SpanLink>,
}

/// Encodes rows into a record batch that matches the canonical span-block
/// schema.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn encode_span_rows(rows: &[SpanRow]) -> Result<RecordBatch> {
    encode_span_rows_with_promoted_attrs(rows, &[])
}

pub(crate) fn new_str_list() -> ListBuilder<StringBuilder> {
    ListBuilder::new(StringBuilder::new())
}

pub(crate) fn new_str_list_list() -> ListBuilder<ListBuilder<StringBuilder>> {
    ListBuilder::new(new_str_list())
}

pub(crate) fn promoted_attr_value<'a>(attrs: &'a [SpanAttr], key: &str) -> Option<&'a AttrValue> {
    attrs
        .iter()
        .find_map(|attr| (attr.key == key && !attr.is_array).then_some(&attr.value))
}

pub(crate) fn append_events(events: &mut ListBuilder<StructBuilder>, rows: &[SpanEvent]) {
    let sb = events.values();
    for event in rows {
        sb.field_builder::<StringBuilder>(0)
            .expect("event name builder")
            .append_value(&event.name);
        sb.field_builder::<Int64Builder>(1)
            .expect("event time builder")
            .append_value(event.time_since_start.nanos_i64());
        append_kv(sb, &event.attrs);
        sb.append(true);
    }
    events.append(true);
}

pub(crate) fn append_links(
    links: &mut ListBuilder<StructBuilder>,
    rows: &[SpanLink],
) -> Result<()> {
    let sb = links.values();
    for link in rows {
        sb.field_builder::<FixedSizeBinaryBuilder>(0)
            .expect("linked trace id builder")
            .append_value(link.linked_trace_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        sb.field_builder::<FixedSizeBinaryBuilder>(1)
            .expect("linked span id builder")
            .append_value(link.linked_span_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        append_kv(sb, &link.attrs);
        sb.append(true);
    }
    links.append(true);
    Ok(())
}

pub(crate) fn new_event_struct_builder() -> StructBuilder {
    new_attributed_struct_builder([
        StructColumn {
            field: Field::new("name", DataType::Utf8, true),
            builder: Box::new(StringBuilder::new()),
        },
        StructColumn {
            field: Field::new("time_since_start_nano", DataType::Int64, true),
            builder: Box::new(Int64Builder::new()),
        },
    ])
}

pub(crate) fn new_link_struct_builder() -> StructBuilder {
    new_attributed_struct_builder([
        StructColumn {
            field: Field::new("linked_trace_id", DataType::FixedSizeBinary(16), true),
            builder: Box::new(FixedSizeBinaryBuilder::new(16)),
        },
        StructColumn {
            field: Field::new("linked_span_id", DataType::FixedSizeBinary(8), true),
            builder: Box::new(FixedSizeBinaryBuilder::new(8)),
        },
    ])
}

/// One column of a struct: its field and the builder that fills it.
pub(crate) struct StructColumn {
    pub(crate) field: Field,
    pub(crate) builder: Box<dyn ArrayBuilder>,
}

/// A struct builder for a span event or link: its two leading columns, then
/// the attribute keys, values and typed attributes every such struct carries.
pub(crate) fn new_attributed_struct_builder(leading: [StructColumn; 2]) -> StructBuilder {
    let [first, second] = leading;
    StructBuilder::new(
        Fields::from(vec![
            first.field,
            second.field,
            Field::new(
                SCOL_ATTR_KEYS,
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                SCOL_ATTR_VALUE,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ))),
                true,
            ),
            Field::new("attr_typed", DataType::Utf8, true),
        ]),
        vec![
            first.builder,
            second.builder,
            Box::new(new_str_list()),
            Box::new(new_str_list_list()),
            Box::new(StringBuilder::new()),
        ],
    )
}

/// Indexed text projections and the lossless typed values share one logical source.
pub(crate) fn append_kv(sb: &mut StructBuilder, attrs: &[SpanAttr]) {
    let keys = sb
        .field_builder::<ListBuilder<StringBuilder>>(2)
        .expect("attr key list builder");
    for attr in attrs {
        keys.values().append_value(&attr.key);
    }
    keys.append(true);
    let values = sb
        .field_builder::<ListBuilder<ListBuilder<StringBuilder>>>(3)
        .expect("attr value list builder");
    for attr in attrs {
        let projected = match &attr.value {
            AttrValue::Unsupported(_) => Vec::new(),
            AttrValue::Str(values) => values.clone(),
            AttrValue::Int(values) => values.iter().map(ToString::to_string).collect(),
            AttrValue::Double(values) => values.iter().map(ToString::to_string).collect(),
            AttrValue::Bool(values) => values.iter().map(ToString::to_string).collect(),
        };
        for value in projected {
            values.values().values().append_value(value);
        }
        values.values().append(true);
    }
    values.append(true);
    sb.field_builder::<StringBuilder>(4)
        .expect("typed attribute payload builder")
        .append_value(
            serde_json::to_string(attrs).expect("typed attributes have total JSON serialization"),
        );
}

pub(crate) fn append_attrs(
    attrs: &[SpanAttr],
    keys: &mut ListBuilder<StringBuilder>,
    is_array: &mut ListBuilder<BooleanBuilder>,
    str_values: &mut ListBuilder<ListBuilder<StringBuilder>>,
    int_values: &mut ListBuilder<ListBuilder<Int64Builder>>,
    double_values: &mut ListBuilder<ListBuilder<Float64Builder>>,
    bool_values: &mut ListBuilder<ListBuilder<BooleanBuilder>>,
) {
    for attr in attrs {
        keys.values().append_value(&attr.key);
        is_array.values().append_value(attr.is_array);

        match &attr.value {
            AttrValue::Unsupported(_) => {}
            AttrValue::Str(values) => {
                for value in values {
                    str_values.values().values().append_value(value);
                }
            }
            AttrValue::Int(values) => {
                for &value in values {
                    int_values.values().values().append_value(value);
                }
            }
            AttrValue::Double(values) => {
                for &value in values {
                    double_values.values().values().append_value(value);
                }
            }
            AttrValue::Bool(values) => {
                for &value in values {
                    bool_values.values().values().append_value(value);
                }
            }
        }

        str_values.values().append(true);
        int_values.values().append(true);
        double_values.values().append(true);
        bool_values.values().append(true);
    }
    keys.append(true);
    is_array.append(true);
    str_values.append(true);
    int_values.append(true);
    double_values.append(true);
    bool_values.append(true);
}

/// Encodes rows into a record batch with configured attribute columns promoted
/// out of the generic attribute lists.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn encode_span_rows_with_promoted_attrs(
    rows: &[SpanRow],
    promoted_attrs: &[PromotedSpanAttr],
) -> Result<RecordBatch> {
    let mut span_columns = SpanColumnBuilders::with_capacity(rows.len());
    let mut promoted = promoted_attrs
        .iter()
        .map(PromotedAttrBuilder::new)
        .collect::<Vec<_>>();
    let mut attr_keys = new_str_list();
    let mut attr_is_array = ListBuilder::new(BooleanBuilder::new());
    let mut attr_value = new_str_list_list();
    let mut attr_value_int = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    let mut attr_value_double = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
    let mut attr_value_unsupported = new_str_list();
    let mut attr_value_bool = ListBuilder::new(ListBuilder::new(BooleanBuilder::new()));
    let mut events = ListBuilder::new(new_event_struct_builder());
    let mut links = ListBuilder::new(new_link_struct_builder());

    for row in rows {
        span_columns.append(row)?;
        for builder in &mut promoted {
            builder.append(&row.attrs);
        }

        append_attrs(
            &row.attrs,
            &mut attr_keys,
            &mut attr_is_array,
            &mut attr_value,
            &mut attr_value_int,
            &mut attr_value_double,
            &mut attr_value_bool,
        );
        for attr in &row.attrs {
            attr_value_unsupported.values().append_option(
                if let super::AttrValue::Unsupported(value) = &attr.value {
                    Some(value)
                } else {
                    None
                },
            );
        }
        attr_value_unsupported.append(true);
        append_events(&mut events, &row.events);
        append_links(&mut links, &row.links)?;
    }

    let mut columns = span_columns.finish();
    columns.extend(promoted.into_iter().map(PromotedAttrBuilder::finish));
    columns.extend([
        Arc::new(attr_keys.finish()) as ArrayRef,
        Arc::new(attr_is_array.finish()) as ArrayRef,
        Arc::new(attr_value.finish()) as ArrayRef,
        Arc::new(attr_value_int.finish()) as ArrayRef,
        Arc::new(attr_value_double.finish()) as ArrayRef,
        Arc::new(attr_value_bool.finish()) as ArrayRef,
        Arc::new(attr_value_unsupported.finish()) as ArrayRef,
        Arc::new(events.finish()) as ArrayRef,
        Arc::new(links.finish()) as ArrayRef,
    ]);

    RecordBatch::try_new(
        span_block_schema_with_promoted_attrs(promoted_attrs),
        columns,
    )
    .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))
}
