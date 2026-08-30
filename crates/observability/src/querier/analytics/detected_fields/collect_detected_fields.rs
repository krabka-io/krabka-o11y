use super::{
    BTreeMap, DetectedFieldStats, DetectedFieldsParams, HeaderMap, HttpQueryError, QuerierState,
    QueryError, TimeRange, active_log_delete_filters, authorized_tenant,
    detect_detected_level_field, detect_json_fields, detect_logfmt_fields,
    detect_structured_metadata_fields, is_deleted_log_entry, parse_query, plan_stream_query,
    read_log_block, read_log_block_from_object_store, validate_loki_volume_query_range_limit,
    validate_query_bytes_limit, validate_query_length_limit, validate_query_range_limit,
    validate_query_series_limit,
};

pub(crate) async fn collect_detected_fields(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &DetectedFieldsParams,
) -> Result<BTreeMap<String, DetectedFieldStats>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_loki_volume_query_range_limit(time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;

    let mut fields = BTreeMap::new();
    let mut scanned_lines = 0_usize;
    for block in &plan.blocks {
        if scanned_lines >= params.line_limit {
            break;
        }
        let rows = if let Some(cold_store) = &state.cold_store {
            read_log_block_from_object_store(
                cold_store.store.as_ref(),
                &cold_store.prefix,
                &block.key,
            )
            .await?
        } else {
            read_log_block(&state.root, &block.key)?
        };
        for row in rows {
            if scanned_lines >= params.line_limit {
                break;
            }
            if !plan.fingerprints.contains(&row.series_fingerprint)
                || row.timestamp_ns < plan.time_range.start_ns
                || row.timestamp_ns > plan.time_range.end_ns
            {
                continue;
            }
            let labels = state
                .label_index
                .labels_for(tenant, row.series_fingerprint)
                .ok_or(QueryError::MissingSeriesLabels {
                    tenant: tenant.to_string(),
                    fingerprint: row.series_fingerprint,
                })?;
            if is_deleted_log_entry(
                &delete_filters,
                labels,
                &row.line,
                &row.structured_metadata,
                row.timestamp_ns,
            ) {
                continue;
            }
            if !plan
                .query
                .matches_with_fields(labels, &row.line, &row.structured_metadata)
            {
                continue;
            }
            scanned_lines += 1;
            detect_detected_level_field(&mut fields, labels, &row.line);
            detect_structured_metadata_fields(&mut fields, &row.structured_metadata);
            detect_json_fields(&mut fields, &row.line);
            detect_logfmt_fields(&mut fields, &row.line);
        }
    }

    Ok(fields)
}
