use super::{Array, COL_DURATION, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START, COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_ID, RecordBatch, SpanRef, Time, TimeExt, TraceSpans, TraceqlError, attr_values, deduplicate_trace_spans, event_values, fixed, fixed_value, int32_value, int64_value, link_values, nullable_fixed_value, resource_attr_values, string_value};

pub(crate) fn trace_from_batches(
    trace_id: &[u8; 16],
    batches: Vec<RecordBatch>,
) -> Result<Option<TraceSpans>, TraceqlError> {
    let mut root_service_name = String::new();
    let mut root_trace_name = String::new();
    let mut resource_attributes = Vec::new();
    let mut spans = Vec::new();

    for batch in batches {
        let trace_ids = fixed(&batch, COL_TRACE_ID)?;
        for row in 0..batch.num_rows() {
            if trace_ids.value(row) != trace_id {
                continue;
            }
            if root_service_name.is_empty() {
                root_service_name = string_value(&batch, COL_ROOT_SERVICE_NAME, row)?;
            }
            if root_trace_name.is_empty() {
                root_trace_name = string_value(&batch, COL_ROOT_SPAN_NAME, row)?;
            }
            if resource_attributes.is_empty() {
                resource_attributes = resource_attr_values(&batch, row)?;
            }
            spans.push(SpanRef {
                span_id: fixed_value::<8>(&batch, COL_SPAN_ID, row)?,
                parent_span_id: nullable_fixed_value::<8>(&batch, COL_PARENT_SPAN_ID, row)?,
                name: string_value(&batch, COL_NAME, row)?,
                kind: int32_value(&batch, COL_KIND, row)?,
                nested_set_left: int32_value(&batch, COL_NS_LEFT, row)?,
                nested_set_right: int32_value(&batch, COL_NS_RIGHT, row)?,
                nested_set_parent: int32_value(&batch, COL_PARENT_ID, row)?,
                start_time_unix_nano: u64::try_from(int64_value(&batch, COL_START, row)?)
                    .unwrap_or(0),
                duration: Time::from_nanos(int64_value(&batch, COL_DURATION, row)?),
                status_code: int32_value(&batch, COL_STATUS_CODE, row)?,
                status_message: string_value(&batch, COL_STATUS_MESSAGE, row)?,
                instrumentation_name: string_value(&batch, COL_INSTRUMENTATION_NAME, row)?,
                instrumentation_version: string_value(&batch, COL_INSTRUMENTATION_VERSION, row)?,
                resource_attributes: resource_attr_values(&batch, row)?,
                attributes: attr_values(&batch, row)?,
                events: event_values(&batch, row)?,
                links: link_values(&batch, row)?,
            });
        }
    }

    deduplicate_trace_spans(&mut spans);
    Ok((!spans.is_empty()).then_some(TraceSpans {
        trace_id: *trace_id,
        root_service_name,
        root_trace_name,
        resource_attributes,
        spans,
    }))
}
