//! Promoted attribute columns, the dynamic half of a span block's schema.
//!
//! `--promote-span-attr` and `--promote-resource-attr` append one
//! `attr.<key>` column per configured attribute to every block the builder
//! writes. The configuration can change between flushes, so a block's promoted
//! columns are a property of the block, not of the running process: these
//! helpers read them back off a block's own schema and reconcile blocks that
//! disagree.

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, StringArray,
        StringDictionaryBuilder,
    },
    datatypes::{DataType, Field, Int32Type, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use krabka_blockstore::{
    PromotedSpanAttr, PromotedSpanAttrType, SCOL_ATTR_IS_ARRAY, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE,
    SCOL_ATTR_VALUE_BOOL, SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT, SCOL_PROMOTED_ATTR_PREFIX,
    span_block_schema,
};

use crate::error::TracesError;

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::check;
    use krabka_blockstore::span_block_schema_with_promoted_attrs;

    use super::*;
    use crate::span::{
        AttrValue, KeyValue, Span, SpanKind, StatusCode,
        batch::{span_batch, span_batch_with_promoted_attrs},
    };

    fn span(attrs: Vec<KeyValue>) -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "GET /".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 100,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: attrs,
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: "test".into(),
            instrumentation_version: String::new(),
        }
    }

    /// The four promoted types are recovered from the column a block carries,
    /// which is what lets a block be read under a configuration that no longer
    /// matches. The string case is the one worth pinning: the write path
    /// dictionary-encodes it, so the round trip has to survive a type that is
    /// not simply `Utf8`.
    #[test]
    fn every_promoted_column_type_is_recovered_from_the_field_it_wrote() {
        for attr in [
            PromotedSpanAttr::string("k"),
            PromotedSpanAttr::int("k"),
            PromotedSpanAttr::double("k"),
            PromotedSpanAttr::bool("k"),
        ] {
            let field = Field::new(attr.column_name(), attr.data_type(), true);
            check!(promoted_span_attr_from_field(&field) == Some(attr));
        }
    }

    /// A column that is neither a base column nor a promoted one means this
    /// build cannot account for every column of the block. Compaction would
    /// otherwise drop it.
    #[test]
    fn a_column_that_is_neither_base_nor_promoted_is_rejected() {
        let mut fields = span_block_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(Field::new("mystery", DataType::Int32, true));
        let schema = Schema::new(fields);

        let err = block_promoted_attrs(&schema).expect_err("the column is unaccounted for");
        check!(err.to_string().contains("mystery"));
    }

    /// The promoted attributes of a block are exactly the ones it was written
    /// with, in the order the schema carries them.
    #[test]
    fn a_blocks_promoted_attributes_are_read_back_off_its_schema() {
        let attrs = vec![
            PromotedSpanAttr::string("http.method"),
            PromotedSpanAttr::int("http.status_code"),
        ];
        let schema = span_block_schema_with_promoted_attrs(&attrs);

        check!(block_promoted_attrs(&schema).expect("a written schema") == attrs);
        check!(block_promoted_attrs(&span_block_schema()).expect("the base schema") == vec![]);
    }

    /// Blocks written either side of a flag edit merge into the union of their
    /// promoted columns, so neither input's dedicated column is dropped.
    #[test]
    fn merging_inputs_written_under_different_flags_unions_their_columns() {
        let one = span_batch_with_promoted_attrs(
            &[span(vec![KeyValue {
                key: "http.method".into(),
                value: AttrValue::Str("GET".into()),
            }])],
            &[PromotedSpanAttr::string("http.method")],
        )
        .expect("a promoted batch");
        let other = span_batch_with_promoted_attrs(
            &[span(vec![KeyValue {
                key: "http.status_code".into(),
                value: AttrValue::Int(200),
            }])],
            &[PromotedSpanAttr::int("http.status_code")],
        )
        .expect("a promoted batch");
        let plain = span_batch(&[span(vec![])]).expect("a plain batch");

        check!(
            merged_promoted_attrs(&[one, plain, other]).expect("the union")
                == vec![
                    PromotedSpanAttr::string("http.method"),
                    PromotedSpanAttr::int("http.status_code"),
                ]
        );
    }

    /// One key promoted at two types has no column that holds both. Picking
    /// either would reinterpret the other block's values, so the merge fails
    /// instead.
    #[test]
    fn merging_one_key_promoted_at_two_types_is_an_error() {
        let attr = KeyValue {
            key: "code".into(),
            value: AttrValue::Int(200),
        };
        let as_int = span_batch_with_promoted_attrs(
            &[span(vec![attr.clone()])],
            &[PromotedSpanAttr::int("code")],
        )
        .expect("a promoted batch");
        let as_string = span_batch_with_promoted_attrs(
            &[span(vec![attr])],
            &[PromotedSpanAttr::string("code")],
        )
        .expect("a promoted batch");

        let err = merged_promoted_attrs(&[as_int, as_string]).expect_err("the types conflict");
        check!(err.to_string().contains("code"));
    }

    /// A batch written before an attribute was promoted still yields the
    /// dedicated column, rebuilt from the generic attribute lists that carry
    /// the same values. Aligning it must not null the column out.
    #[test]
    fn a_column_missing_from_a_batch_is_rebuilt_from_its_generic_attributes() {
        let batch = span_batch(&[
            span(vec![KeyValue {
                key: "http.method".into(),
                value: AttrValue::Str("GET".into()),
            }]),
            span(vec![KeyValue {
                key: "other".into(),
                value: AttrValue::Str("POST".into()),
            }]),
        ])
        .expect("a plain batch");
        let attrs = vec![PromotedSpanAttr::string("http.method")];
        let schema = span_block_schema_with_promoted_attrs(&attrs);

        let aligned = align_batch_to_block_schema(&batch, &schema).expect("an aligned batch");

        check!(aligned.schema() == schema);
        let column = aligned
            .column_by_name("attr.http.method")
            .expect("the promoted column");
        let column = column
            .as_any()
            .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
            .expect("a dictionary column");
        let values = column
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string dictionary values");
        let key = usize::try_from(column.keys().value(0)).expect("a dictionary key");
        check!(values.value(key) == "GET", "the value is not nulled out");
        check!(column.is_null(1), "a row without the attribute stays null");
    }

    /// Every promoted type rebuilds from the generic lists, and a row whose
    /// attribute holds an array stays null, matching what the write path
    /// would have stored.
    #[test]
    fn each_promoted_type_rebuilds_from_the_generic_attribute_lists() {
        let kv = |key: &str, value: AttrValue| KeyValue {
            key: key.into(),
            value,
        };
        let batch = span_batch(&[
            span(vec![
                kv("s", AttrValue::Str("v".into())),
                kv("i", AttrValue::Int(7)),
                kv("d", AttrValue::Double(1.5)),
                kv("b", AttrValue::Bool(true)),
            ]),
            // The same key twice is how the block encoder records an
            // array-valued attribute.
            span(vec![kv("i", AttrValue::Int(1)), kv("i", AttrValue::Int(2))]),
        ])
        .expect("a plain batch");

        let int = promoted_attr_column(&batch, &PromotedSpanAttr::int("i")).expect("an int column");
        check!(
            int.as_any().downcast_ref::<Int64Array>()
                == Some(&Int64Array::from(vec![Some(7), None])),
            "the array-valued attribute of the second row is not promoted"
        );

        let double =
            promoted_attr_column(&batch, &PromotedSpanAttr::double("d")).expect("a double column");
        check!(
            double.as_any().downcast_ref::<Float64Array>()
                == Some(&Float64Array::from(vec![Some(1.5), None]))
        );

        let boolean =
            promoted_attr_column(&batch, &PromotedSpanAttr::bool("b")).expect("a bool column");
        check!(
            boolean.as_any().downcast_ref::<BooleanArray>()
                == Some(&BooleanArray::from(vec![Some(true), None]))
        );

        // A key promoted at a type the row does not hold stays null rather
        // than borrowing another attribute's value.
        let mistyped =
            promoted_attr_column(&batch, &PromotedSpanAttr::int("s")).expect("an int column");
        check!(mistyped.null_count() == mistyped.len());
    }
}

mod align_batch_to_block_schema;
mod block_promoted_attrs;
mod merged_promoted_attrs;
mod promoted_attr_column;
mod promoted_attr_slots;
mod promoted_span_attr_from_field;
mod promoted_values;
mod row_attr_slot;

pub(crate) use align_batch_to_block_schema::align_batch_to_block_schema;
pub(crate) use block_promoted_attrs::block_promoted_attrs;
pub(crate) use merged_promoted_attrs::merged_promoted_attrs;
pub(crate) use promoted_attr_column::promoted_attr_column;
use promoted_attr_slots::promoted_attr_slots;
pub(crate) use promoted_span_attr_from_field::promoted_span_attr_from_field;
use promoted_values::promoted_values;
use row_attr_slot::row_attr_slot;
