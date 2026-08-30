use super::*;

pub(crate) fn collect_trace_intrinsic_values(
    trace: &TraceSpans,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    match tag {
        "trace:duration" => {
            if let Some(duration) = trace_duration(trace) {
                values.insert(("duration".to_string(), duration.nanos_i64().to_string()));
            }
        }
        "trace:id" => {
            values.insert(("string".to_string(), hex::encode(trace.trace_id)));
        }
        "trace:rootName" => {
            values.insert(("string".to_string(), trace.root_trace_name.clone()));
        }
        "trace:rootService" => {
            values.insert(("string".to_string(), trace.root_service_name.clone()));
        }
        _ => {}
    }
}
