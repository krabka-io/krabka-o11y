use super::*;

pub(crate) struct NestedIntrinsicRows {
    pub(crate) indices: UInt32Array,
    pub(crate) event_name: ArrayRef,
    pub(crate) event_time_since_start: ArrayRef,
    pub(crate) link_trace_id: ArrayRef,
    pub(crate) link_span_id: ArrayRef,
    pub(crate) attr_columns: BTreeMap<String, ArrayRef>,
}

pub(crate) fn nested_intrinsic_rows(
    batch: &RecordBatch,
    matchers: &[SpanMatcher],
    attr_columns: &[(String, NestedAttrColumn)],
) -> Result<NestedIntrinsicRows, TraceqlError> {
    let mut event_name = StringBuilder::new();
    let mut event_time_since_start = Int64Builder::new();
    let mut link_trace_id = FixedSizeBinaryBuilder::with_capacity(batch.num_rows(), 16);
    let mut link_span_id = FixedSizeBinaryBuilder::with_capacity(batch.num_rows(), 8);
    let mut attr_builders = attr_columns
        .iter()
        .map(|(column, attr)| (column.clone(), *attr, StringBuilder::new()))
        .collect::<Vec<_>>();
    let mut row_indices = Vec::new();
    for row in 0..batch.num_rows() {
        let events = matching_events_for_scan(batch, row, matchers)?;
        let links = matching_links_for_scan(batch, row, matchers)?;
        for event in &events {
            for link in &links {
                row_indices
                    .push(u32::try_from(row).map_err(|err| {
                        TraceqlError::Store(format!("row index overflow: {err}"))
                    })?);
                append_nested_event(event.as_ref(), &mut event_name, &mut event_time_since_start);
                append_nested_link(link.as_ref(), &mut link_trace_id, &mut link_span_id)?;
                for (_, attr, builder) in &mut attr_builders {
                    append_nested_attr(event.as_ref(), link.as_ref(), *attr, builder);
                }
            }
        }
    }
    let attr_columns = attr_builders
        .into_iter()
        .map(|(column, _, mut builder)| (column, Arc::new(builder.finish()) as ArrayRef))
        .collect();
    Ok(NestedIntrinsicRows {
        indices: UInt32Array::from(row_indices),
        event_name: Arc::new(event_name.finish()),
        event_time_since_start: Arc::new(event_time_since_start.finish()),
        link_trace_id: Arc::new(link_trace_id.finish()),
        link_span_id: Arc::new(link_span_id.finish()),
        attr_columns,
    })
}
