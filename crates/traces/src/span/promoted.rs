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

/// The promoted attributes a merge of `batches` must carry.
///
/// Compaction inputs need not agree: an operator can edit
/// `--promote-span-attr` between two flushes, and the blocks either side of
/// that edit are compacted together. The merged schema is the union of the
/// inputs' promoted attributes, in first-seen order, so no input's dedicated
/// column is dropped.
///
/// Two inputs that promote the same key at different types have no single
/// column that can hold both, so that is an error rather than a silent choice
/// of one.
pub(crate) fn merged_promoted_attrs(
    batches: &[RecordBatch],
) -> Result<Vec<PromotedSpanAttr>, TracesError> {
    let mut merged: Vec<PromotedSpanAttr> = Vec::new();
    for batch in batches {
        for attr in block_promoted_attrs(&batch.schema())? {
            match merged.iter().find(|seen| seen.key == attr.key) {
                Some(seen) if seen.value_type != attr.value_type => {
                    return Err(TracesError::Block(format!(
                        "promoted attribute `{}` is {:?} in one input block and {:?} in \
                         another; recompact the inputs one configuration at a time",
                        attr.key, seen.value_type, attr.value_type
                    )));
                }
                Some(_) => {}
                None => merged.push(attr),
            }
        }
    }
    Ok(merged)
}

/// Rebuild a promoted attribute's dedicated column from a batch that lacks it.
///
/// The write path writes a promoted attribute twice: once into its dedicated
/// column and once into the generic attribute lists that every block carries.
/// The generic copy is therefore a complete source for the dedicated one, so a
/// block written before the attribute was promoted still yields the same
/// column a block written after it would have.
///
/// Filling the column with nulls instead would read correctly today, because
/// the query path falls back to the generic lists per row, but it would leave
/// a column that says "absent" where the value is present. Anything that later
/// pushes a filter down onto the dedicated column would silently lose rows.
pub(crate) fn promoted_attr_column(
    batch: &RecordBatch,
    attr: &PromotedSpanAttr,
) -> Result<ArrayRef, TracesError> {
    let slots = promoted_attr_slots(batch, &attr.key)?;
    match attr.value_type {
        PromotedSpanAttrType::String => {
            let values = promoted_values::<StringArray, _>(
                batch,
                SCOL_ATTR_VALUE,
                &slots,
                |values, idx| values.value(idx).to_string(),
            )?;
            let mut builder = StringDictionaryBuilder::<Int32Type>::new();
            for value in values {
                builder.append_option(value);
            }
            Ok(Arc::new(builder.finish()))
        }
        PromotedSpanAttrType::Int => {
            let values = promoted_values::<Int64Array, _>(
                batch,
                SCOL_ATTR_VALUE_INT,
                &slots,
                Int64Array::value,
            )?;
            Ok(Arc::new(values.into_iter().collect::<Int64Array>()))
        }
        PromotedSpanAttrType::Double => {
            let values = promoted_values::<Float64Array, _>(
                batch,
                SCOL_ATTR_VALUE_DOUBLE,
                &slots,
                Float64Array::value,
            )?;
            Ok(Arc::new(values.into_iter().collect::<Float64Array>()))
        }
        PromotedSpanAttrType::Bool => {
            let values = promoted_values::<BooleanArray, _>(
                batch,
                SCOL_ATTR_VALUE_BOOL,
                &slots,
                BooleanArray::value,
            )?;
            Ok(Arc::new(values.into_iter().collect::<BooleanArray>()))
        }
    }
}

/// Recover the promoted-attribute declaration a span-block column was written
/// from.
///
/// The block builder writes one `attr.<key>` column per configured promoted
/// attribute, so a block's own Parquet schema carries the configuration it was
/// written under. Reading it back is what lets the compactor and the querier
/// work on a block whose promoted attributes no longer match today's flags.
///
/// String columns are dictionary-encoded on the write path, but a reader may
/// hand back a plain or view-typed string, so every string spelling maps to
/// the same declaration. A column the write path could not have produced
/// returns `None`.
pub(crate) fn promoted_span_attr_from_field(field: &Field) -> Option<PromotedSpanAttr> {
    let key = field.name().strip_prefix(SCOL_PROMOTED_ATTR_PREFIX)?;
    let value_type = match field.data_type() {
        DataType::Dictionary(_, value) => match value.as_ref() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                PromotedSpanAttrType::String
            }
            _ => return None,
        },
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => PromotedSpanAttrType::String,
        DataType::Int64 => PromotedSpanAttrType::Int,
        DataType::Float64 => PromotedSpanAttrType::Double,
        DataType::Boolean => PromotedSpanAttrType::Bool,
        _ => return None,
    };
    Some(PromotedSpanAttr {
        key: key.to_string(),
        value_type,
    })
}

/// The position of `key` in one row's generic attribute list, or `None` when
/// the row does not carry it.
///
/// Array-valued attributes are skipped, because the write path promotes only
/// scalars: [`krabka_blockstore::encode_span_rows_with_promoted_attrs`] leaves
/// the dedicated column null for a row whose matching attribute holds a list.
pub(crate) fn row_attr_slot(
    keys: &ListArray,
    is_array: Option<&ListArray>,
    row: usize,
    key: &str,
) -> Result<Option<usize>, TracesError> {
    if keys.is_null(row) {
        return Ok(None);
    }
    let row_keys = keys.value(row);
    let row_keys = row_keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TracesError::Block("attribute keys row is not Utf8".into()))?;

    let row_flags = match is_array {
        Some(flags) if !flags.is_null(row) => Some(flags.value(row)),
        _ => None,
    };
    let row_flags =
        match row_flags.as_ref() {
            Some(flags) => Some(flags.as_any().downcast_ref::<BooleanArray>().ok_or_else(
                || TracesError::Block("attribute is-array row is not Boolean".into()),
            )?),
            None => None,
        };

    for idx in 0..row_keys.len() {
        if row_keys.is_null(idx) || row_keys.value(idx) != key {
            continue;
        }
        let holds_array = row_flags
            .is_some_and(|flags| idx < flags.len() && !flags.is_null(idx) && flags.value(idx));
        if holds_array {
            continue;
        }
        return Ok(Some(idx));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::check;
    use krabka_blockstore::span_block_schema_with_promoted_attrs;

    use super::*;
    use crate::span::{
        AttrValue, KeyValue, Span,
        batch::{span_batch, span_batch_with_promoted_attrs},
        test_span::api_server_span,
    };

    fn span(attrs: Vec<KeyValue>) -> Span {
        Span {
            span_attrs: attrs,
            instrumentation_scope: "test".into(),
            ..api_server_span()
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
mod promoted_values;
use promoted_values::promoted_values;

/// Project a span-block batch onto `schema`, rebuilding any promoted
/// attribute column the batch does not already carry.
///
/// Compaction merges blocks written under different promoted-attribute
/// configurations, and the writer requires every batch to match the output
/// schema exactly. Columns are matched by name, so an input keeps its own
/// values wherever it has them.
pub(crate) fn align_batch_to_block_schema(
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, TracesError> {
    crate::align_record_batch::align_record_batch(
        batch,
        schema,
        |_field, column| Ok(column.clone()),
        |field| {
            let attr = promoted_span_attr_from_field(field).ok_or_else(|| {
                TracesError::Block(format!("span block is missing column `{}`", field.name()))
            })?;
            promoted_attr_column(batch, &attr)
        },
        |err| TracesError::Block(err.to_string()),
    )
}

/// The promoted attributes a span block was written with, read from the
/// block's own schema.
///
/// A block's schema is the base span schema with one `attr.<key>` column per
/// promoted attribute appended, so every column that is not a base column must
/// be a promoted one. A column that is neither is a block this build cannot
/// interpret, and silently ignoring it would drop data on compaction.
pub(crate) fn block_promoted_attrs(schema: &Schema) -> Result<Vec<PromotedSpanAttr>, TracesError> {
    let base = span_block_schema();
    let mut attrs = Vec::new();
    for field in schema.fields() {
        if base.column_with_name(field.name()).is_some() {
            continue;
        }
        let attr = promoted_span_attr_from_field(field).ok_or_else(|| {
            TracesError::Block(format!(
                "span block column `{}` of type {} is neither a base column nor a \
                 promoted attribute column",
                field.name(),
                field.data_type()
            ))
        })?;
        attrs.push(attr);
    }
    Ok(attrs)
}

/// Where `key` sits in each row's generic attribute list.
///
/// One entry per row of `batch`, so a caller can walk the rows once and read
/// the value out of whichever typed value column its declared type names.
pub(crate) fn promoted_attr_slots(
    batch: &RecordBatch,
    key: &str,
) -> Result<Vec<Option<usize>>, TracesError> {
    let keys = batch
        .column_by_name(SCOL_ATTR_KEYS)
        .and_then(|column| column.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| TracesError::Block(format!("{SCOL_ATTR_KEYS} is not a list")))?;
    let is_array = batch
        .column_by_name(SCOL_ATTR_IS_ARRAY)
        .and_then(|column| column.as_any().downcast_ref::<ListArray>());

    (0..batch.num_rows())
        .map(|row| row_attr_slot(keys, is_array, row, key))
        .collect()
}
