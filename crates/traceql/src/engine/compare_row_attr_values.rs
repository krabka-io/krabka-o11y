use super::{AttrValue, CompareRow, Scope};

pub(crate) fn compare_row_attr_values<'a>(
    row: &'a CompareRow,
    scope: &Scope,
    key: &str,
) -> Vec<&'a AttrValue> {
    let mut out = Vec::new();
    let want_span = matches!(scope, Scope::Both | Scope::Span);
    let want_resource = matches!(scope, Scope::Both | Scope::Resource);
    if want_span {
        out.extend(
            row.raw_span_attrs
                .iter()
                .filter(|(attr_key, _)| attr_key == key)
                .map(|(_, value)| value),
        );
    }
    if want_resource {
        out.extend(
            row.raw_resource_attrs
                .iter()
                .filter(|(attr_key, _)| attr_key == key)
                .map(|(_, value)| value),
        );
    }
    out
}
