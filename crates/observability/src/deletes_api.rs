async fn create_delete_request(
    State(state): State<CompactorDeleteState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    match execute_create_delete_request(&state, &headers, raw_query.as_deref(), &body) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}

fn execute_create_delete_request(
    state: &CompactorDeleteState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<(), HttpQueryError> {
    let tenant = tenant(headers)?.to_string();
    let raw_params = request_query_or_form_body(raw_query, body)?;
    let params = parse_create_delete_request_params(Some(raw_params.as_str()))?;
    parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;

    let mut requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests.next_id += 1;
    let request_id = format!("delete-{}", requests.next_id);
    requests.requests.push(CompactorDeleteRequest {
        tenant,
        request_id,
        query: params.query,
        start_time: params.start_time,
        end_time: params.end_time,
        status: "received".to_string(),
        created_at: current_unix_time_ns() / 1_000_000_000,
    });
    drop(requests);
    state.delete_requests.persist()?;
    Ok(())
}

async fn list_delete_requests(
    State(state): State<CompactorDeleteState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_list_delete_requests(&state, &headers, raw_query.as_deref()) {
        Ok(requests) => json_response(StatusCode::OK, &json!(requests)),
        Err(error) => error.into_response(),
    }
}

fn execute_list_delete_requests(
    state: &CompactorDeleteState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Vec<CompactorDeleteRequestResponse>, HttpQueryError> {
    let tenant = tenant(headers)?;
    let params = parse_list_delete_requests_params(raw_query)?;
    let requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    Ok(requests
        .requests
        .iter()
        .filter(|request| request.tenant == tenant)
        .filter(|request| delete_request_overlaps_filter(request, &params))
        .map(|request| CompactorDeleteRequestResponse {
            request_id: request.request_id.clone(),
            start_time: request.start_time,
            end_time: request.end_time,
            query: request.query.clone(),
            status: request.status.clone(),
            created_at: request.created_at,
        })
        .collect())
}

async fn cancel_delete_request(
    State(state): State<CompactorDeleteState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_cancel_delete_request(&state, &headers, raw_query.as_deref()) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}

fn execute_cancel_delete_request(
    state: &CompactorDeleteState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<(), HttpQueryError> {
    let tenant = tenant(headers)?.to_string();
    let request_id = parse_cancel_delete_request_params(raw_query)?;
    let mut requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests
        .requests
        .retain(|request| request.tenant != tenant || request.request_id != request_id);
    drop(requests);
    state.delete_requests.persist()?;
    Ok(())
}

fn request_query_or_form_body(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    match raw_query {
        Some(raw_query) if !raw_query.is_empty() => Ok(raw_query.to_string()),
        _ if !body.is_empty() => form_body_query(body),
        _ => Err(HttpQueryError::MissingQueryParameter("query")),
    }
}

fn parse_create_delete_request_params(
    raw_query: Option<&str>,
) -> Result<CreateDeleteRequestParams, HttpQueryError> {
    let mut query = None;
    let mut start_time = None;
    let mut end_time = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("query"));
    };

    for pair in split_query_param_pairs(raw_query, &["query", "start", "end", "max_interval"]) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;
        match key.as_str() {
            "query" => query = Some(value),
            "start" => start_time = Some(parse_loki_delete_timestamp_query_param("start", &value)?),
            "end" => end_time = Some(parse_loki_delete_timestamp_query_param("end", &value)?),
            "max_interval" => {
                parse_loki_duration_query_param("max_interval", &value)?;
            }
            _ => {}
        }
    }

    let start_time = start_time.ok_or(HttpQueryError::MissingQueryParameter("start"))?;
    let end_time = end_time.unwrap_or_else(|| current_unix_time_ns() / 1_000_000_000);
    if end_time < start_time {
        return Err(HttpQueryError::InvalidQueryParameter {
            name: "end",
            value: "end must be greater than or equal to start".to_string(),
        });
    }

    Ok(CreateDeleteRequestParams {
        query: query.ok_or(HttpQueryError::MissingQueryParameter("query"))?,
        start_time,
        end_time,
    })
}

fn parse_list_delete_requests_params(
    raw_query: Option<&str>,
) -> Result<ListDeleteRequestsParams, HttpQueryError> {
    let mut start_time = None;
    let mut end_time = None;
    if let Some(raw_query) = raw_query {
        for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = decode_form_component(key)?;
            let value = decode_form_component(value)?;
            match key.as_str() {
                "start" => {
                    start_time = Some(parse_loki_delete_timestamp_query_param("start", &value)?);
                }
                "end" => end_time = Some(parse_loki_delete_timestamp_query_param("end", &value)?),
                _ => {}
            }
        }
    }
    if start_time.is_some() != end_time.is_some() {
        return Err(HttpQueryError::InvalidQueryParameter {
            name: "start",
            value: "start and end must be provided together".to_string(),
        });
    }
    Ok(ListDeleteRequestsParams {
        start_time,
        end_time,
    })
}

fn parse_cancel_delete_request_params(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let mut request_id = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("request_id"));
    };
    for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_form_component(key)?;
        let value = decode_form_component(value)?;
        match key.as_str() {
            "request_id" => request_id = Some(value),
            "force" => match value.as_str() {
                "true" | "false" => {}
                _ => {
                    return Err(HttpQueryError::InvalidQueryParameter {
                        name: "force",
                        value,
                    });
                }
            },
            _ => {}
        }
    }
    request_id.ok_or(HttpQueryError::MissingQueryParameter("request_id"))
}

fn parse_loki_delete_timestamp_query_param(
    name: &'static str,
    value: &str,
) -> Result<i64, HttpQueryError> {
    if let Ok(seconds) = value.parse::<i64>() {
        return Ok(seconds);
    }
    if let Some(timestamp_ns) = parse_decimal_seconds_timestamp(value) {
        return Ok(timestamp_ns / 1_000_000_000);
    }
    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .map(time::OffsetDateTime::unix_timestamp)
        .ok_or_else(|| HttpQueryError::InvalidTimestampQueryParameter {
            name,
            value: value.to_string(),
        })
}

fn delete_request_overlaps_filter(
    request: &CompactorDeleteRequest,
    params: &ListDeleteRequestsParams,
) -> bool {
    match (params.start_time, params.end_time) {
        (Some(start_time), Some(end_time)) => {
            request.end_time >= start_time && request.start_time <= end_time
        }
        _ => true,
    }
}

fn active_log_delete_filters(
    state: &QuerierState,
    tenant: &str,
    query_range: TimeRange,
) -> Result<Vec<ActiveLogDeleteFilter>, HttpQueryError> {
    let Some(delete_requests) = &state.delete_requests else {
        return Ok(Vec::new());
    };
    Ok(active_log_delete_filters_from_requests(
        delete_requests,
        tenant,
        query_range,
    )?)
}

fn active_log_delete_filters_from_requests(
    delete_requests: &SharedLogDeleteRequests,
    tenant: &str,
    query_range: TimeRange,
) -> Result<Vec<ActiveLogDeleteFilter>, ActiveLogDeleteFilterError> {
    delete_requests.refresh()?;
    let requests = delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests
        .requests
        .iter()
        .filter(|request| request.tenant == tenant)
        .filter_map(|request| {
            delete_request_time_range(request)
                .ok()
                .filter(|range| ranges_overlap(*range, query_range))
                .map(|range| (request, range))
        })
        .map(|(request, time_range)| {
            let query = parse_query(&request.query).map_err(|source| {
                ActiveLogDeleteFilterError::Parse {
                    query: request.query.clone(),
                    source,
                }
            })?;
            Ok(ActiveLogDeleteFilter { time_range, query })
        })
        .collect()
}

fn delete_request_time_range(
    request: &CompactorDeleteRequest,
) -> Result<TimeRange, ActiveLogDeleteFilterError> {
    let start_ns =
        request
            .start_time
            .checked_mul(1_000_000_000)
            .ok_or(BlockStoreError::InvalidTimeRange {
                start_ns: request.start_time,
                end_ns: request.end_time,
            })?;
    let end_ns = request
        .end_time
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(999_999_999))
        .ok_or(BlockStoreError::InvalidTimeRange {
            start_ns: request.start_time,
            end_ns: request.end_time,
        })?;
    TimeRange::new(start_ns, end_ns).map_err(ActiveLogDeleteFilterError::from)
}

fn ranges_overlap(left: TimeRange, right: TimeRange) -> bool {
    left.end_ns >= right.start_ns && left.start_ns <= right.end_ns
}

