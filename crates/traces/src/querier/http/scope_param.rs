use super::{Uri, TagScope, query_param, parse_tag_scope};

pub(crate) fn scope_param(uri: &Uri) -> Result<Option<TagScope>, &'static str> {
    query_param(uri, "scope")
        .map(|scope| parse_tag_scope(&scope).ok_or("invalid scope"))
        .transpose()
}
