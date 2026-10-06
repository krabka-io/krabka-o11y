use axum::http::Uri;
use krabka_traceql::TagScope;

use crate::query_param::query_param;

pub(crate) fn scope_param(uri: &Uri) -> Result<Option<TagScope>, &'static str> {
    query_param(uri, "scope")
        .map(|scope| TagScope::from_name(&scope).ok_or("invalid scope"))
        .transpose()
}
