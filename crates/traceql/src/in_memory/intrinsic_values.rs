use std::collections::BTreeSet;

use krabka_units::convert::TimeExt as _;

use super::{InputSpan, NestedSet, StoredTrace, TypedValue, collect_span_field_values};
use crate::engine::bytes_to_hex;

// Collect Span Intrinsic Values
pub(crate) fn collect_span_intrinsic_values(
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    tag: &str,
    values: &mut BTreeSet<TypedValue>,
) {
    let nested = nested_sets.get(idx);
    match tag {
        "span:childCount" => {
            if let Some(nested) = nested {
                let count = nested_sets
                    .iter()
                    .filter(|other| other.parent_id == nested.left)
                    .count();
                values.insert(TypedValue {
                    type_: "int".to_string(),
                    value: count.to_string(),
                });
            }
        }
        "span:nestedSetLeft" => {
            if let Some(nested) = nested {
                values.insert(TypedValue {
                    type_: "int".to_string(),
                    value: nested.left.to_string(),
                });
            }
        }
        "span:nestedSetParent" => {
            if let Some(nested) = nested {
                values.insert(TypedValue {
                    type_: "int".to_string(),
                    value: nested.parent_id.to_string(),
                });
            }
        }
        "span:nestedSetRight" => {
            if let Some(nested) = nested {
                values.insert(TypedValue {
                    type_: "int".to_string(),
                    value: nested.right.to_string(),
                });
            }
        }
        _ => collect_span_field_values(span.into(), tag, values),
    }
}

// Collect Trace Intrinsic Values
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
