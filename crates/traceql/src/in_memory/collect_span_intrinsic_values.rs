use super::*;

pub(crate) fn collect_span_intrinsic_values(
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    let nested = nested_sets.get(idx);
    match tag {
        "span:childCount" => {
            if let Some(nested) = nested {
                let count = nested_sets
                    .iter()
                    .filter(|other| other.parent_id == nested.left)
                    .count();
                values.insert(("int".to_string(), count.to_string()));
            }
        }
        "span:duration" => {
            values.insert((
                "duration".to_string(),
                span.duration.nanos_i64().to_string(),
            ));
        }
        "span:id" => {
            values.insert(("string".to_string(), bytes_to_hex(&span.span_id)));
        }
        "span:kind" => {
            values.insert(("int".to_string(), span.kind.to_string()));
        }
        "span:name" => {
            values.insert(("string".to_string(), span.name.clone()));
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(("string".to_string(), bytes_to_hex(&parent_id)));
            }
        }
        "span:status" => {
            values.insert(("int".to_string(), span.status_code.to_string()));
        }
        "span:statusMessage" => {
            if !span.status_message.is_empty() {
                values.insert(("string".to_string(), span.status_message.clone()));
            }
        }
        "span:nestedSetLeft" => {
            if let Some(nested) = nested {
                values.insert(("int".to_string(), nested.left.to_string()));
            }
        }
        "span:nestedSetParent" => {
            if let Some(nested) = nested {
                values.insert(("int".to_string(), nested.parent_id.to_string()));
            }
        }
        "span:nestedSetRight" => {
            if let Some(nested) = nested {
                values.insert(("int".to_string(), nested.right.to_string()));
            }
        }
        "instrumentation:name" if !span.instrumentation_name.is_empty() => {
            values.insert(("string".to_string(), span.instrumentation_name.clone()));
        }
        "instrumentation:version" if !span.instrumentation_version.is_empty() => {
            values.insert(("string".to_string(), span.instrumentation_version.clone()));
        }
        _ => {}
    }
}
