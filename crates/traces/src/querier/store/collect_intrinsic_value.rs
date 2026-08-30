use super::{RecordBatch, BTreeSet, TraceqlError, insert_i64_value, COL_DURATION, bytes_to_hex, fixed, COL_SPAN_ID, insert_i32_value, COL_KIND, insert_string_value, COL_NAME, COL_CHILD_COUNT, nullable_fixed_value, COL_PARENT_SPAN_ID, COL_STATUS_CODE, string_value, COL_STATUS_MESSAGE, Array, COL_NS_LEFT, COL_PARENT_ID, COL_NS_RIGHT, COL_TRACE_DURATION, COL_TRACE_ID, COL_ROOT_SPAN_NAME, COL_ROOT_SERVICE_NAME, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION};

pub(crate) fn collect_intrinsic_value(
    batch: &RecordBatch,
    row: usize,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) -> Result<(), TraceqlError> {
    match tag {
        "span:duration" => {
            insert_i64_value(batch, row, values, "duration", COL_DURATION)?;
        }
        "span:id" => {
            values.insert((
                "string".to_string(),
                bytes_to_hex(fixed(batch, COL_SPAN_ID)?.value(row)),
            ));
        }
        "span:kind" => {
            insert_i32_value(batch, row, values, COL_KIND)?;
        }
        "span:name" => {
            insert_string_value(batch, row, values, COL_NAME)?;
        }
        "span:childCount" => {
            insert_i32_value(batch, row, values, COL_CHILD_COUNT)?;
        }
        "span:parentID" => {
            if let Some(parent_id) = nullable_fixed_value::<8>(batch, COL_PARENT_SPAN_ID, row)? {
                values.insert(("string".to_string(), bytes_to_hex(&parent_id)));
            }
        }
        "span:status" => {
            insert_i32_value(batch, row, values, COL_STATUS_CODE)?;
        }
        "span:statusMessage" => {
            let message = string_value(batch, COL_STATUS_MESSAGE, row)?;
            if !message.is_empty() {
                values.insert(("string".to_string(), message));
            }
        }
        "span:nestedSetLeft" => {
            insert_i32_value(batch, row, values, COL_NS_LEFT)?;
        }
        "span:nestedSetParent" | "span:Parent" => {
            insert_i32_value(batch, row, values, COL_PARENT_ID)?;
        }
        "span:nestedSetRight" => {
            insert_i32_value(batch, row, values, COL_NS_RIGHT)?;
        }
        "trace:duration" => {
            insert_i64_value(batch, row, values, "duration", COL_TRACE_DURATION)?;
        }
        "trace:id" => {
            values.insert((
                "string".to_string(),
                bytes_to_hex(fixed(batch, COL_TRACE_ID)?.value(row)),
            ));
        }
        "trace:rootName" => {
            insert_string_value(batch, row, values, COL_ROOT_SPAN_NAME)?;
        }
        "trace:rootService" => {
            insert_string_value(batch, row, values, COL_ROOT_SERVICE_NAME)?;
        }
        "instrumentation:name" => {
            insert_string_value(batch, row, values, COL_INSTRUMENTATION_NAME)?;
        }
        "instrumentation:version" => {
            insert_string_value(batch, row, values, COL_INSTRUMENTATION_VERSION)?;
        }
        _ => {}
    }
    Ok(())
}
