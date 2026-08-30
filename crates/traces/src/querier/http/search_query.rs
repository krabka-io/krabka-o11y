use super::*;

pub(crate) fn search_query(uri: &Uri) -> Result<Option<String>, &'static str> {
    if let Some(query) = query_param(uri, "q") {
        return Ok(Some(query));
    }
    query_param(uri, "tags")
        .map(|tags| tags_to_traceql(&tags).ok_or("invalid query parameter tags"))
        .transpose()
}
