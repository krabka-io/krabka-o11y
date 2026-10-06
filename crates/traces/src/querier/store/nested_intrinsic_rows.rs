use arrow::{
    array::{
        BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder, StructArray,
    },
    datatypes::Field,
};

use super::{
    Arc, ArrayRef, AttrValue, BTreeMap, FixedSizeBinaryBuilder, NestedAttrColumn, RecordBatch,
    SpanMatcher, TraceqlError, UInt32Array, append_nested_attr, append_nested_event,
    append_nested_link, matching_events_for_scan, matching_links_for_scan,
};

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
        .map(|(column, attr)| (column.clone(), *attr, Vec::new()))
        .collect::<Vec<_>>();
    let mut row_indices = Vec::new();
    let filtered_events = matchers.iter().any(super::is_event_matcher);
    let filtered_links = matchers.iter().any(super::is_link_matcher);
    for row in 0..batch.num_rows() {
        let all_events = super::event_values(batch, row)?;
        let all_links = super::link_values(batch, row)?;
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
                    let projected_event = if filtered_events {
                        event.as_ref()
                    } else {
                        all_events
                            .iter()
                            .find(|event| event.attributes.iter().any(|(key, _)| key == attr.key))
                    };
                    let projected_link = if filtered_links {
                        link.as_ref()
                    } else {
                        all_links
                            .iter()
                            .find(|link| link.attributes.iter().any(|(key, _)| key == attr.key))
                    };
                    append_nested_attr(projected_event, projected_link, *attr, builder);
                }
            }
        }
    }
    let attr_columns = attr_builders
        .into_iter()
        .map(|(column, _, values)| {
            let mut strings = ListBuilder::new(StringBuilder::new());
            let mut ints = ListBuilder::new(Int64Builder::new());
            let mut floats = ListBuilder::new(Float64Builder::new());
            let mut bools = ListBuilder::new(BooleanBuilder::new());
            let mut is_array = BooleanBuilder::new();
            for values in values {
                is_array.append_value(matches!(values.as_slice(), [AttrValue::Array(_)]));
                let values = if let [AttrValue::Array(elements)] = values.as_slice() {
                    elements.clone()
                } else {
                    values
                };
                for value in values {
                    match value {
                        AttrValue::Unsupported(_) => {}
                        AttrValue::Array(_) => {
                            unreachable!("OTLP nested arrays are rejected before block encoding")
                        }
                        AttrValue::Str(value) => strings.values().append_value(value),
                        AttrValue::Int(value) => ints.values().append_value(value),
                        AttrValue::Float(value) => floats.values().append_value(value),
                        AttrValue::Bool(value) => bools.values().append_value(value),
                    }
                }
                strings.append(true);
                ints.append(true);
                floats.append(true);
                bools.append(true);
            }
            let arrays = [
                ("is_array", Arc::new(is_array.finish()) as ArrayRef),
                ("string", Arc::new(strings.finish()) as ArrayRef),
                ("int", Arc::new(ints.finish()) as ArrayRef),
                ("float", Arc::new(floats.finish()) as ArrayRef),
                ("bool", Arc::new(bools.finish()) as ArrayRef),
            ];
            let array = StructArray::from(
                arrays
                    .into_iter()
                    .map(|(name, array)| {
                        (
                            Arc::new(Field::new(name, array.data_type().clone(), true)),
                            array,
                        )
                    })
                    .collect::<Vec<_>>(),
            );
            (column, Arc::new(array) as ArrayRef)
        })
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
