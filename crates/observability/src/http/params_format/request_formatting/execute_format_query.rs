use super::*;

pub(crate) fn execute_format_query(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let query = parse_format_query_param(raw_query)?;
    format_logql_query(&query)
}
