use super::{AttrValue, CompareRow, Scope};

pub(crate) fn compare_row_attr_values<'a>(
    row: &'a CompareRow,
    scope: &Scope,
    key: &str,
) -> Vec<&'a AttrValue> {
    let prefixed;
    let key = match scope {
        Scope::Event => {
            prefixed = format!("{}{key}", super::EVENT_ATTR_PREFIX);
            &prefixed
        }
        Scope::Link => {
            prefixed = format!("{}{key}", super::LINK_ATTR_PREFIX);
            &prefixed
        }
        Scope::Instrumentation => {
            prefixed = format!("{}{key}", super::INSTRUMENTATION_ATTR_PREFIX);
            &prefixed
        }
        _ => key,
    };
    let mut out = Vec::new();
    let want_span = matches!(
        scope,
        Scope::Both | Scope::Span | Scope::Event | Scope::Link | Scope::Instrumentation
    );
    let want_resource = matches!(scope, Scope::Both | Scope::Resource);
    if want_span {
        out.extend(
            row.raw_span_attrs
                .iter()
                .filter(|(attr_key, _)| attr_key == key)
                .map(|(_, value)| value),
        );
    }
    // Tempo vParquet AttributeFor checks span attributes before falling back
    // to resource attributes for an unscoped key.
    if want_resource && (!matches!(scope, Scope::Both) || out.is_empty()) {
        out.extend(
            row.raw_resource_attrs
                .iter()
                .filter(|(attr_key, _)| attr_key == key)
                .map(|(_, value)| value),
        );
    }
    if matches!(scope, Scope::Both) && out.is_empty() {
        for prefix in [
            super::EVENT_ATTR_PREFIX,
            super::LINK_ATTR_PREFIX,
            super::INSTRUMENTATION_ATTR_PREFIX,
        ] {
            let prefixed = format!("{prefix}{key}");
            out.extend(
                row.raw_span_attrs
                    .iter()
                    .filter(|(key, _)| key == &prefixed)
                    .map(|(_, value)| value),
            );
            if !out.is_empty() {
                break;
            }
        }
    }
    // Pinned vParquet5 attributeCollector ignores IsArray: empty lists are
    // nil and a single element is scalar. Keep original shape for retrieval.
    out.into_iter()
        .filter_map(|value| match value {
            AttrValue::Unsupported(_) => None,
            AttrValue::Array(values) if values.is_empty() => None,
            AttrValue::Array(values) if values.len() == 1 => values.first(),
            value => Some(value),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn unscoped_values_prefer_span_and_fall_back_only_when_absent() {
        let row = CompareRow {
            ts: super::super::UnixNano(0),
            attrs: Vec::new(),
            raw_span_attrs: vec![("shared".into(), AttrValue::Int(1))],
            raw_resource_attrs: vec![
                ("shared".into(), AttrValue::Str("1".into())),
                ("fallback".into(), AttrValue::Bool(true)),
            ],
            name: None,
            status_code: None,
            status_message: None,
            kind: None,
            duration: None,
            columns: std::collections::BTreeMap::new(),
        };
        assert!(compare_row_attr_values(&row, &Scope::Both, "shared") == vec![&AttrValue::Int(1)]);
        assert!(
            compare_row_attr_values(&row, &Scope::Resource, "shared")
                == vec![&AttrValue::Str("1".into())]
        );
        assert!(
            compare_row_attr_values(&row, &Scope::Both, "fallback") == vec![&AttrValue::Bool(true)]
        );
    }
}
