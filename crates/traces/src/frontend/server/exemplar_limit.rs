use super::{Uri, query_param};

pub(crate) fn exemplar_limit(uri: &Uri) -> Option<usize> {
    match query_param(uri, "exemplars").as_deref() {
        Some("false" | "0") => Some(0),
        Some("true") | None => None,
        Some(value) => value.parse().ok().or(None),
    }
}
