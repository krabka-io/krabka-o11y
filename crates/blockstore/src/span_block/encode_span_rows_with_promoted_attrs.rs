use super::*;

/// Encodes rows into a record batch with configured attribute columns promoted
/// out of the generic attribute lists.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn encode_span_rows_with_promoted_attrs(
    rows: &[SpanRow],
    promoted_attrs: &[PromotedSpanAttr],
) -> Result<RecordBatch> {
    let mut span_columns = SpanColumnBuilders::new();
    let mut promoted = promoted_attrs
        .iter()
        .map(PromotedAttrBuilder::new)
        .collect::<Vec<_>>();
    let mut attr_keys = new_str_list();
    let mut attr_is_array = ListBuilder::new(BooleanBuilder::new());
    let mut attr_value = new_str_list_list();
    let mut attr_value_int = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    let mut attr_value_double = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
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
        Arc::new(events.finish()) as ArrayRef,
        Arc::new(links.finish()) as ArrayRef,
    ]);

    RecordBatch::try_new(
        span_block_schema_with_promoted_attrs(promoted_attrs),
        columns,
    )
    .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))
}
