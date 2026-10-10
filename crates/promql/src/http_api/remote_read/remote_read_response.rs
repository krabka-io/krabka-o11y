use super::{
    ApiError, BTreeMap, Labels, MetricStore, PrometheusApiState, RemoteReadSampleSink,
    SeriesFingerprint, TenantId, append_remote_read_exemplars, append_remote_read_float_samples,
    append_remote_read_histogram_samples, enforce_query_range_limit, enforce_selected_series_limit,
    pb, remote_read_matchers, validate_timestamp_range,
};

pub(crate) async fn remote_read_response<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    request: pb::v1::ReadRequest,
) -> Result<pb::v1::ReadResponse, ApiError> {
    let mut results = Vec::with_capacity(request.queries.len());
    for mut query in request.queries {
        validate_timestamp_range(query.start_timestamp_ms, query.end_timestamp_ms)?;
        enforce_query_range_limit(
            state,
            tenant,
            query.start_timestamp_ms,
            query.end_timestamp_ms,
        )?;
        // Mimir lets each positive hint endpoint override the outer range.
        if let Some(hints) = &query.hints {
            if hints.start_ms > 0 {
                query.start_timestamp_ms = hints.start_ms;
            }
            if hints.end_ms > 0 {
                query.end_timestamp_ms = hints.end_ms;
            }
            validate_timestamp_range(query.start_timestamp_ms, query.end_timestamp_ms)?;
            enforce_query_range_limit(
                state,
                tenant,
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )?;
        }
        let matchers = remote_read_matchers(&query.matchers)?;
        let labels = state
            .store
            .series(
                tenant.as_str(),
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
                tenant.as_str(),
                &matchers,
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )
            .await
            .map_err(ApiError::from)?;

        let mut by_fp = BTreeMap::<SeriesFingerprint, pb::v1::TimeSeries>::new();
        let mut returned_samples = 0_u64;

        let mut sink = RemoteReadSampleSink {
            state,
            tenant,
            labels_by_fp: &labels_by_fp,
            series_by_fp: &mut by_fp,
            returned_samples: &mut returned_samples,
        };
        if let Some(float_table) = scan.float_table.clone() {
            append_remote_read_float_samples(&mut sink, &scan, &float_table).await?;
        }
        if let Some(histogram_table) = scan.histogram_table.clone() {
            append_remote_read_histogram_samples(&mut sink, &scan, &histogram_table).await?;
        }

        append_remote_read_exemplars(
            state.store.as_ref(),
            tenant.as_str(),
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
