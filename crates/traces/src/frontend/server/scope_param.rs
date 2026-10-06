use super::{Uri, query_param};

pub(crate) fn scope_param(uri: &Uri) -> Result<Option<krabka_traceql::TagScope>, &'static str> {
    query_param(uri, "scope")
        .map(|s| krabka_traceql::TagScope::from_name(&s).ok_or("invalid scope"))
        .transpose()
}
