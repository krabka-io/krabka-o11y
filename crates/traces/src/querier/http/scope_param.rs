use super::{TagScope, Uri, query_param, tag_scope_from_name};

pub(crate) fn scope_param(uri: &Uri) -> Result<Option<TagScope>, &'static str> {
    query_param(uri, "scope")
        .map(|scope| tag_scope_from_name(&scope).ok_or("invalid scope"))
        .transpose()
}
