use super::{Uri, query_param, parse_scope};

pub(crate) fn scope_param(uri: &Uri) -> Result<Option<krabka_traceql::TagScope>, &'static str> {
    query_param(uri, "scope")
        .map(|s| parse_scope(&s).ok_or("invalid scope"))
        .transpose()
}
