use super::{MetricStore, PrometheusApiState, pb, ApiError, validate_timestamp_range, remote_read_matchers, enforce_selected_series_limit, BTreeMap, SeriesFingerprint, Labels, append_remote_read_float_samples, append_remote_read_histogram_samples, append_remote_read_exemplars};

pub(crate) async fn remote_read_response<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    request: pb::v1::ReadRequest,
) -> Result<pb::v1::ReadResponse, ApiError> {
    let mut results = Vec::with_capacity(request.queries.len());
    for query in request.queries {
        validate_timestamp_range(query.start_timestamp_ms, query.end_timestamp_ms)?;
        let matchers = remote_read_matchers(&query.matchers)?;
        let labels = state
            .store
            .series(
                tenant,
                &matchers,
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )
            .await
            .map_err(ApiError::from)?;
        enforce_selected_series_limit(state, tenant, labels.len())?;
        let mut labels_by_fp = labels
            .into_iter()
            .map(|labels| (labels.fingerprint(), labels))
            .collect::<BTreeMap<SeriesFingerprint, Labels>>();
        let scan = state
            .store
            .scan(
                tenant,
                &matchers,
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )
            .await
            .map_err(ApiError::from)?;

        let mut by_fp = BTreeMap::<SeriesFingerprint, pb::v1::TimeSeries>::new();
        let mut returned_samples = 0_u64;

        if let Some(float_table) = scan.float_table.clone() {
            append_remote_read_float_samples(
                state,
                tenant,
                &scan,
                &float_table,
                &labels_by_fp,
                &mut by_fp,
                &mut returned_samples,
            )
            .await?;
        }

        if let Some(histogram_table) = scan.histogram_table.clone() {
            append_remote_read_histogram_samples(
                state,
                tenant,
                &scan,
                &histogram_table,
                &labels_by_fp,
                &mut by_fp,
                &mut returned_samples,
            )
            .await?;
        }

        append_remote_read_exemplars(
            state.store.as_ref(),
            tenant,
            &matchers,
            query.start_timestamp_ms,
            query.end_timestamp_ms,
            &mut labels_by_fp,
            &mut by_fp,
        )
        .await?;

        results.push(pb::v1::QueryResult {
            timeseries: by_fp.into_values().collect(),
        });
    }
    Ok(pb::v1::ReadResponse { results })
}
