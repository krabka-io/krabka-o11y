use super::*;

pub(crate) fn collect_trace_intrinsic_values(
    trace: &StoredTrace,
    tag: &str,
    values: &mut BTreeSet<TypedValue>,
) {
    match tag {
        "trace:duration" => {
            values.insert(TypedValue {
                type_: "duration".to_string(),
                value: trace.trace_duration.nanos_i64().to_string(),
            });
        }
        "trace:id" => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: bytes_to_hex(&trace.trace_id),
            });
        }
        "trace:rootName" => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: trace.root_span_name.clone(),
            });
        }
        "trace:rootService" => {
            values.insert(TypedValue {
                type_: "string".to_string(),
                value: trace.root_service_name.clone(),
            });
        }
        _ => {}
    }
}
