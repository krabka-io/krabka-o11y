use super::{
    BTreeSet, HeaderMap, HttpQueryError, QuerierState, RequestSecurity, SeriesParams, series_data,
};

pub(crate) async fn label_names_data(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Vec<String>, HttpQueryError> {
    let mut names = BTreeSet::new();
    for labels in series_data(state, security, headers, params).await? {
        names.extend(labels.keys().cloned());
    }

    Ok(names.into_iter().collect::<Vec<_>>())
}
