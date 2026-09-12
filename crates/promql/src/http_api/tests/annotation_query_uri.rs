use url::form_urlencoded;

// Builds a GET URI for `route` with `query` and a fixed single-step evaluation
// window, so an instant query and a range query cover the same instant.
pub(crate) fn annotation_query_uri(route: &str, query: &str) -> String {
    let form = form_urlencoded::Serializer::new(String::new())
        .append_pair("query", query)
        .append_pair("time", "0")
        .append_pair("start", "0")
        .append_pair("end", "0")
        .append_pair("step", "60")
        .finish();
    format!("{route}?{form}")
}
