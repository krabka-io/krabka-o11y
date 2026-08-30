use super::*;

pub(crate) fn collect_trace_intrinsic_values(
    trace: &StoredTrace,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    match tag {
        "trace:duration" => {
            values.insert((
                "duration".to_string(),
                trace.trace_duration.nanos_i64().to_string(),
            ));
        }
        "trace:id" => {
            values.insert(("string".to_string(), bytes_to_hex(&trace.trace_id)));
        }
        "trace:rootName" => {
            values.insert(("string".to_string(), trace.root_span_name.clone()));
        }
        "trace:rootService" => {
            values.insert(("string".to_string(), trace.root_service_name.clone()));
        }
        _ => {}
    }
}
