use super::super::{MetadataQuery, record_metadata_query};
use crate::{HeaderMap, QuerierState, RawQuery, RequestSecurity, Response, State};

pub(crate) async fn label_names(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    record_metadata_query(
        state,
        security,
        headers,
        raw_query,
        "labels",
        MetadataQuery::LabelNames,
    )
    .await
}
