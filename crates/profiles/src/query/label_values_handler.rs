use super::*;

pub(crate) async fn label_values_handler<S>(
    state: Extension<Arc<QuerierState<S>>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::querier::v1::LabelValuesRequest>,
) -> Result<ConnectResponse<pb::querier::v1::LabelValuesResponse>, ConnectError>
where
    S: ProfileStore,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        "label_values",
        label_values_inner(state, headers, req),
    )
    .await
}
