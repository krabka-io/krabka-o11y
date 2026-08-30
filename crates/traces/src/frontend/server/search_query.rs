use super::*;

/// `q` (`TraceQL`) or the legacy `tags` logfmt form.
pub(crate) fn search_query(uri: &Uri) -> Result<Option<String>, &'static str> {
    if let Some(q) = query_param(uri, "q") {
        return Ok(Some(q));
    }
    query_param(uri, "tags")
        .map(|tags| tags_to_traceql(&tags).ok_or("invalid query parameter tags"))
        .transpose()
}
