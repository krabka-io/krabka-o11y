use super::*;

pub(crate) fn metadata_selectors(
    params: &SeriesParams,
) -> Result<Vec<krabka_logql::StreamQuery>, HttpQueryError> {
    params
        .matchers
        .iter()
        .map(|matcher| {
            parse_query(matcher).map_err(|source| HttpQueryError::LokiParse {
                query: matcher.clone(),
                source,
            })
        })
        .collect()
}
