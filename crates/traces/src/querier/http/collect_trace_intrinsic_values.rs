use super::*;

pub(crate) fn collect_trace_intrinsic_values(
    trace: &TraceSpans,
    tag: &str,
    values: &mut BTreeSet<TypedValue>,
) {
    match tag {
        "trace:duration" => {
            if let Some(duration) = trace_duration(trace) {
                values.insert(TypedValue::new(
                    "duration",
                    duration.nanos_i64().to_string(),
                ));
            }
        }
        "trace:id" => {
            values.insert(TypedValue::new("string", hex::encode(trace.trace_id)));
        }
        "trace:rootName" => {
            values.insert(TypedValue::new("string", trace.root_trace_name.clone()));
        }
        "trace:rootService" => {
            values.insert(TypedValue::new("string", trace.root_service_name.clone()));
        }
        _ => {}
    }
}
