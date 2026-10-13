use super::{BTreeSet, InputSpan, NestedSet, TypedValue, collect_span_field_values};

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
