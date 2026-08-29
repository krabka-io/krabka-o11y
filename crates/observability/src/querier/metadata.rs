#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) async fn execute_label_names_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let data = label_names_data(state, headers, params).await?;
    Ok(if data.is_empty() {
        loki_sparse_success()
    } else {
        loki_success(data)
    })
}

pub(crate) async fn execute_api_prom_label_names_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let values = label_names_data(state, headers, params).await?;
    Ok(if values.is_empty() {
        json_response(StatusCode::OK, &json!({}))
    } else {
        json_response(
            StatusCode::OK,
            &json!({
                "values": values,
            }),
        )
    })
}

pub(crate) async fn label_names_data(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Vec<String>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let state = state
        .with_request_tenant_index(tenant, metadata_index_range(params)?)
        .await?;
    let mut names = BTreeSet::new();
    for labels in metadata_label_sets(&state, tenant, params).await? {
        names.extend(labels.keys().cloned());
    }

    Ok(names.into_iter().collect::<Vec<_>>())
}

pub(crate) async fn execute_label_values_query(
    state: &QuerierState,
    headers: &HeaderMap,
    name: &str,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    let data = label_values_data(state, headers, name, params).await?;
    Ok(if data.is_empty() {
        loki_sparse_success()
    } else {
        loki_success(data)
    })
}

pub(crate) async fn label_values_data(
    state: &QuerierState,
    headers: &HeaderMap,
    name: &str,
    params: &SeriesParams,
) -> Result<Vec<String>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let state = state
        .with_request_tenant_index(tenant, metadata_index_range(params)?)
        .await?;
    let mut values = BTreeSet::new();
    for labels in metadata_label_sets(&state, tenant, params).await? {
        if let Some(value) = labels.get(name) {
            values.insert(value.clone());
        }
    }

    Ok(values.into_iter().collect::<Vec<_>>())
}

pub(crate) fn metadata_time_range(
    params: &SeriesParams,
) -> Result<Option<TimeRange>, HttpQueryError> {
    if params.start.is_none() && params.end.is_none() && params.since.is_none() {
        return Ok(None);
    }

    let end = if params.start.is_none() && params.since.is_some() && params.end.is_none() {
        Some(current_unix_time_ns())
    } else {
        params.end
    };
    optional_start_end_range(params.start, params.since, end).map(Some)
}

pub(crate) fn metadata_index_range(params: &SeriesParams) -> Result<TimeRange, HttpQueryError> {
    let Some(time_range) = metadata_time_range(params)? else {
        let end_ns = current_unix_time_ns();
        return TimeRange::new(
            end_ns.saturating_sub(LOKI_METADATA_DEFAULT_INDEX_RANGE.nanos_i64()),
            end_ns,
        )
        .map_err(HttpQueryError::from);
    };
    validate_loki_volume_query_range_limit(time_range)?;
    Ok(time_range)
}

pub(crate) async fn metadata_label_sets(
    state: &QuerierState,
    tenant: &str,
    params: &SeriesParams,
) -> Result<Vec<Labels>, HttpQueryError> {
    let time_range = metadata_time_range(params)?;
    let time_fingerprints = if let Some(time_range) = time_range {
        Some(metadata_fingerprints_in_time_range(state, tenant, time_range).await?)
    } else {
        None
    };

    let selectors = metadata_selectors(params)?;
    let mut label_sets = BTreeSet::new();

    for (fingerprint, labels) in state.label_index.tenant_series(tenant) {
        if time_fingerprints
            .as_ref()
            .is_none_or(|fingerprints| fingerprints.contains(&fingerprint))
            && metadata_labels_match_selectors(&labels, &selectors)
        {
            label_sets.insert(metadata_visible_labels(&labels));
        }
    }

    if let Some(hot_tail) = &state.hot_tail {
        // Prune to the metadata window when one is supplied; the per-record bound below
        // (`< start_ns || > end_ns`) is re-applied, so the windowed records are exactly
        // the records a full scan would have kept. With no time range, scan everything.
        let records = match time_range {
            Some(range) => hot_tail
                .source
                .records_in_range(range.start_ns, range.end_ns),
            None => hot_tail.source.records(),
        };
        let frontier = hot_tail.frontier.snapshot();
        for record in records {
            if record.tenant != tenant || frontier.is_compacted(&record) {
                continue;
            }
            if time_range.is_some_and(|range| {
                record.timestamp_ns < range.start_ns || record.timestamp_ns > range.end_ns
            }) {
                continue;
            }
            if metadata_labels_match_selectors(&record.labels, &selectors) {
                label_sets.insert(metadata_visible_labels(&record.labels));
            }
        }
    }

    Ok(label_sets.into_iter().collect())
}

pub(crate) async fn metadata_fingerprints_in_time_range(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
) -> Result<BTreeSet<SeriesFingerprint>, HttpQueryError> {
    let mut fingerprints = BTreeSet::new();
    for block in state.block_index.match_blocks(tenant, time_range, &[]) {
        let rows = if let Some(cold_store) = &state.cold_store {
            read_log_block_from_object_store(
                cold_store.store.as_ref(),
                &cold_store.prefix,
                &block.key,
            )
            .await?
        } else {
            match read_log_block(&state.root, &block.key) {
                Ok(rows) => rows,
                Err(BlockStoreError::Io(source)) if source.kind() == ErrorKind::NotFound => {
                    fingerprints.extend(block.fingerprints);
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        };
        fingerprints.extend(rows.into_iter().filter_map(|row| {
            (time_range.start_ns <= row.timestamp_ns && row.timestamp_ns <= time_range.end_ns)
                .then_some(row.series_fingerprint)
        }));
    }
    Ok(fingerprints)
}

pub(crate) fn metadata_visible_labels(labels: &Labels) -> Labels {
    let mut labels = labels.clone();
    labels.remove("detected_level");
    labels
}

pub(crate) fn metadata_labels_match_selectors(
    labels: &Labels,
    selectors: &[krabka_logql::StreamQuery],
) -> bool {
    if selectors.is_empty() {
        return true;
    }

    selectors.iter().any(|selector| {
        selector
            .matchers
            .iter()
            .all(|matcher| matcher.matches(labels))
    })
}

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

pub(crate) async fn execute_series_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    Ok(loki_success(series_data(state, headers, params).await?))
}

pub(crate) async fn execute_api_prom_series_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Response, HttpQueryError> {
    Ok(loki_success(series_data(state, headers, params).await?))
}

pub(crate) async fn series_data(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &SeriesParams,
) -> Result<Vec<Labels>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let state = state
        .with_request_tenant_index(tenant, metadata_index_range(params)?)
        .await?;
    metadata_label_sets(&state, tenant, params).await
}

pub(crate) fn parse_series_params(raw_query: Option<&str>) -> Result<SeriesParams, HttpQueryError> {
    let mut params = SeriesParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };

    for pair in split_query_param_pairs(
        raw_query,
        &["match[]", "match%5B%5D", "query", "start", "end", "since"],
    ) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;

        match key.as_str() {
            "match[]" | "query" => params.matchers.push(value),
            "start" if params.start.is_none() => {
                params.start = Some(parse_loki_timestamp_query_param("start", &value)?);
            }
            "end" if params.end.is_none() => {
                params.end = Some(parse_loki_timestamp_query_param("end", &value)?);
            }
            "since" if params.since.is_none() => {
                params.since = Some(parse_loki_duration_query_param("since", &value)?);
            }
            _ => {}
        }
    }

    Ok(params)
}
