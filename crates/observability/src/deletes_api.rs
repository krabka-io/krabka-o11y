use crate::{
    ActiveLogDeleteFilter, ActiveLogDeleteFilterError, BlockStoreError, Bytes,
    CompactorDeleteRequest, CompactorDeleteRequestResponse, CompactorDeleteState,
    CreateDeleteRequestParams, DecodedQueryPair, HeaderMap, HttpQueryError,
    ListDeleteRequestsParams, OffsetDateTime, QuerierState, RawQuery, RequestSecurity, Response,
    Rfc3339, SharedLogDeleteRequests, State, StatusCode, TenantErrorSurface, TenantId, TimeRange,
    audit::{
        AuditOutcome, OPERATION_DELETE_REQUEST_CANCEL, OPERATION_DELETE_REQUEST_CREATE,
        RESOURCE_DELETE_REQUEST, RESOURCE_TENANT, resource,
    },
    current_unix_time_ns, decode_query_pair, form_body_query,
    http::params::query_parsing::decoded_query_param_pairs,
    json, json_response, parse_decimal_seconds_timestamp, parse_loki_duration_query_param,
    parse_query, resolve_single_tenant, tenant_header_value,
};

fn audit_delete_request_write<E>(
    security: &RequestSecurity,
    operation: &'static str,
    tenant: &TenantId,
    request_id: &str,
    persisted: &Result<(), E>,
) {
    security.admin_operation(
        operation,
        vec![
            resource(RESOURCE_TENANT, tenant.as_str()),
            resource(RESOURCE_DELETE_REQUEST, request_id),
        ],
        if persisted.is_ok() {
            AuditOutcome::Success
        } else {
            AuditOutcome::Failure
        },
    );
}

pub(crate) fn ranges_overlap(left: TimeRange, right: TimeRange) -> bool {
    left.end_ns >= right.start_ns && left.start_ns <= right.end_ns
}

pub(crate) fn request_query_or_form_body(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    match raw_query {
        Some(raw_query) if !raw_query.is_empty() => Ok(raw_query.to_string()),
        _ if !body.is_empty() => form_body_query(body),
        _ => Err(HttpQueryError::MissingQueryParameter("query")),
    }
}

pub(crate) fn active_log_delete_filters(
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

pub(crate) async fn cancel_delete_request(
    State(state): State<CompactorDeleteState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<StatusCode, HttpQueryError> {
    let tenant = authorized_delete_tenant(&state, &security, &headers).await?;
    execute_cancel_delete_request(&state, &security, &tenant, raw_query.as_deref())?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn create_delete_request(
    State(state): State<CompactorDeleteState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Result<StatusCode, HttpQueryError> {
    let tenant = authorized_delete_tenant(&state, &security, &headers).await?;
    execute_create_delete_request(&state, &security, &tenant, raw_query.as_deref(), &body)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) fn delete_request_overlaps_filter(
    request: &CompactorDeleteRequest,
    params: &ListDeleteRequestsParams,
) -> bool {
    if let Some(start_time) = params.start_time {
        if let Some(end_time) = params.end_time {
            request.end_time >= start_time && request.start_time <= end_time
        } else {
            true
        }
    } else {
        true
    }
}

pub(crate) fn delete_request_time_range(
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

pub(crate) async fn list_delete_requests(
    State(state): State<CompactorDeleteState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, HttpQueryError> {
    let tenant = authorized_delete_tenant(&state, &security, &headers).await?;
    let requests = execute_list_delete_requests(&state, &tenant, raw_query.as_deref())?;
    Ok(json_response(StatusCode::OK, &json!(requests)))
}

pub(crate) fn parse_cancel_delete_request_params(
    raw_query: Option<&str>,
) -> Result<String, HttpQueryError> {
    let mut request_id = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("request_id"));
    };
    for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
        let DecodedQueryPair { key, value } = decode_query_pair(pair)?;
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

pub(crate) fn parse_loki_delete_timestamp_query_param(
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

struct ActiveLogDeleteCandidate<'a> {
    request: &'a super::CompactorDeleteRequest,
    time_range: TimeRange,
}

pub(crate) fn active_log_delete_filters_from_requests(
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
                .map(|time_range| ActiveLogDeleteCandidate {
                    request,
                    time_range,
                })
        })
        .map(|candidate| {
            let query = parse_query(&candidate.request.query).map_err(|source| {
                ActiveLogDeleteFilterError::Parse {
                    query: candidate.request.query.clone(),
                    source,
                }
            })?;
            Ok(ActiveLogDeleteFilter {
                time_range: candidate.time_range,
                query,
            })
        })
        .collect()
}

/// The one tenant a delete-request call names, after the principal's grant and
/// the query authorizer allow it.
///
/// Loki's compactor reads the tenant with `dskit`'s `tenant.TenantID` and
/// answers every tenant error through Go's `http.Error` with 400, except a
/// missing tenant, which gets 401. A tenant outside the principal's grant gets
/// a 403. Both checks run before any parameter is read, so a refused tenant
/// learns nothing about its requests and changes nothing.
pub(crate) async fn authorized_delete_tenant(
    state: &CompactorDeleteState,
    security: &RequestSecurity,
    headers: &HeaderMap,
) -> Result<TenantId, HttpQueryError> {
    let tenant = resolve_single_tenant(tenant_header_value(headers)).map_err(|source| {
        HttpQueryError::Tenant {
            source,
            surface: TenantErrorSurface::Push,
        }
    })?;
    security.authorize_tenant(&tenant)?;
    state
        .query_authorizer
        .check(&security.principal, &tenant)
        .await
        .map_err(|error| {
            security.record_read_refusal(&error);
            HttpQueryError::from(error)
        })?;
    Ok(tenant)
}

/// Removes the delete request `request_id` of `tenant`.
///
/// The audit trail records each cancel with a valid request id, with the
/// outcome of the write. A cancel that the parameters refuse changes nothing,
/// so it records nothing.
pub(crate) fn execute_cancel_delete_request(
    state: &CompactorDeleteState,
    security: &RequestSecurity,
    tenant: &TenantId,
    raw_query: Option<&str>,
) -> Result<(), HttpQueryError> {
    let request_id = parse_cancel_delete_request_params(raw_query)?;
    let mut requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    requests
        .requests
        .retain(|request| request.tenant != tenant.as_str() || request.request_id != request_id);
    let persisted = state.delete_requests.persist(&requests);
    drop(requests);
    audit_delete_request_write(
        security,
        OPERATION_DELETE_REQUEST_CANCEL,
        tenant,
        &request_id,
        &persisted,
    );
    persisted?;
    Ok(())
}

/// Stores a new delete request for `tenant`.
///
/// The audit trail records each request that passes validation, with the
/// request id and the outcome of the write. A request that the parameters
/// refuse changes nothing, so it records nothing.
pub(crate) fn execute_create_delete_request(
    state: &CompactorDeleteState,
    security: &RequestSecurity,
    tenant: &TenantId,
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<(), HttpQueryError> {
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
        tenant: tenant.as_str().to_owned(),
        request_id: request_id.clone(),
        query: params.query,
        start_time: params.start_time,
        end_time: params.end_time,
        status: "received".to_string(),
        created_at: current_unix_time_ns() / 1_000_000_000,
    });
    let persisted = state.delete_requests.persist(&requests);
    drop(requests);
    audit_delete_request_write(
        security,
        OPERATION_DELETE_REQUEST_CREATE,
        tenant,
        &request_id,
        &persisted,
    );
    persisted?;
    Ok(())
}

pub(crate) fn execute_list_delete_requests(
    state: &CompactorDeleteState,
    tenant: &TenantId,
    raw_query: Option<&str>,
) -> Result<Vec<CompactorDeleteRequestResponse>, HttpQueryError> {
    let params = parse_list_delete_requests_params(raw_query)?;
    let requests = state
        .delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    Ok(requests
        .requests
        .iter()
        .filter(|request| request.tenant == tenant.as_str())
        .filter(|request| delete_request_overlaps_filter(request, &params))
        .map(|request| CompactorDeleteRequestResponse {
            user_id: String::new(),
            request_id: request.request_id.clone(),
            start_time: request.start_time,
            end_time: request.end_time,
            query: request.query.clone(),
            status: request.status.clone(),
            created_at: request.created_at,
        })
        .collect())
}

pub(crate) fn parse_create_delete_request_params(
    raw_query: Option<&str>,
) -> Result<CreateDeleteRequestParams, HttpQueryError> {
    let mut query = None;
    let mut start_time = None;
    let mut end_time = None;
    for pair in decoded_query_param_pairs(raw_query, &["query", "start", "end", "max_interval"])? {
        match pair.key.as_str() {
            "query" => query = Some(pair.value),
            "start" => {
                start_time = Some(parse_loki_delete_timestamp_query_param(
                    "start",
                    &pair.value,
                )?);
            }
            "end" => end_time = Some(parse_loki_delete_timestamp_query_param("end", &pair.value)?),
            "max_interval" => {
                parse_loki_duration_query_param("max_interval", &pair.value)?;
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

pub(crate) fn parse_list_delete_requests_params(
    raw_query: Option<&str>,
) -> Result<ListDeleteRequestsParams, HttpQueryError> {
    let mut start_time = None;
    let mut end_time = None;
    if let Some(raw_query) = raw_query {
        for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
            let DecodedQueryPair { key, value } = decode_query_pair(pair)?;
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
