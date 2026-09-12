use crate::{
    Arc, BTreeMap, BTreeSet, BlockDescriptor, ByteSizeExt, CacheKey, HttpQueryError, LabelIndex,
    LokiDirection, LokiStreamEncoding, PlannedQuery, QuerierState, QueryFrontend,
    QueryFrontendAdapter, QueryFrontendError, QueryKind, QueryParams, SeriesFingerprint, TenantId,
    TimeRange, Value, apply_loki_stream_options, execute_http_query_for_tenant_inner, json,
    loki_direction, merge_loki_query_stats, parse_query, plan_stream_query,
    populate_loki_query_execution_stats, transient_object_store_error, validate_loki_interval,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FingerprintBounds {
    pub(crate) min: u64,
    pub(crate) max: u64,
}

#[derive(Clone)]
struct LogsPlannedQuery {
    params: QueryParams,
    bounds: FingerprintBounds,
    planned_at: std::time::Instant,
}

struct LogsQueryFrontendAdapter<'a> {
    state: QuerierState,
    tenant: &'a TenantId,
    encoding: LokiStreamEncoding,
    time_range: TimeRange,
    shards: Vec<FingerprintBounds>,
}

pub(crate) async fn execute_logs_query_frontend(
    state: &QuerierState,
    tenant: &TenantId,
    params: &QueryParams,
    encoding: LokiStreamEncoding,
) -> Result<Value, HttpQueryError> {
    let state = state.with_tenant_limits(tenant);
    let time_range = crate::time_range(params, QueryKind::Range)?;
    let time_range =
        crate::clamp_query_lookback(&state.limits, time_range, crate::current_unix_time_ns());
    crate::validate_loki_range_query_range_limit(&state, QueryKind::Range, time_range)?;
    crate::validate_query_range_limit(&state, time_range)?;
    crate::validate_query_string_bytes_limit(&state, &params.query)?;
    crate::validate_query_entries_limit(&state, params.limit)?;
    crate::validate_loki_query_range_resolution(params, QueryKind::Range, time_range)?;
    validate_loki_interval(params.interval)?;
    let _ = loki_direction(params.direction.as_deref())?;
    let prepared = state
        .with_request_tenant_index(tenant.as_str(), time_range)
        .await?;
    let shards = stream_shards(
        &prepared,
        tenant.as_str(),
        &params.query,
        time_range,
        prepared.query_frontend_target_bytes,
    )?;
    let adapter = LogsQueryFrontendAdapter {
        state: prepared.clone(),
        tenant,
        encoding,
        time_range,
        shards,
    };
    let frontend = QueryFrontend::new(
        Arc::clone(&prepared.query_frontend_cache),
        prepared.query_frontend_options,
    );
    match frontend.execute(&adapter, params).await {
        Ok(value) => Ok(value),
        Err(QueryFrontendError::Adapter(error)) => Err(error),
        Err(QueryFrontendError::Cache(never)) => match never {},
    }
}

#[async_trait::async_trait]
impl QueryFrontendAdapter for LogsQueryFrontendAdapter<'_> {
    type Request = QueryParams;
    type Query = LogsPlannedQuery;
    type Output = Value;
    type Response = Value;
    type Error = HttpQueryError;

    fn plan(&self, request: &Self::Request) -> Result<Vec<PlannedQuery<Self::Query>>, Self::Error> {
        let ranges = split_ranges(self.time_range, self.state.query_frontend_split_ns);
        let partitioned = ranges.len() > 1 || self.shards.len() > 1;
        let mut planned = Vec::with_capacity(ranges.len().saturating_mul(self.shards.len()));
        for range in ranges {
            for bounds in &self.shards {
                let params = planned_query_params(request, range, partitioned);
                let cache_key =
                    logs_cache_key(self.tenant.as_str(), request, range, *bounds, self.encoding);
                planned.push(PlannedQuery {
                    query: LogsPlannedQuery {
                        params,
                        bounds: *bounds,
                        planned_at: std::time::Instant::now(),
                    },
                    cache_key,
                    end_epoch_millis: range.end_ns.div_euclid(1_000_000),
                });
            }
        }
        Ok(planned)
    }

    async fn execute(&self, query: &Self::Query) -> Result<Self::Output, Self::Error> {
        let started = std::time::Instant::now();
        let queue_time = started.saturating_duration_since(query.planned_at);
        let state = state_for_bounds(&self.state, self.tenant.as_str(), query.bounds);
        let mut result = execute_http_query_for_tenant_inner(
            &state,
            self.tenant,
            &query.params,
            QueryKind::Range,
            self.encoding,
        )
        .await?;
        populate_loki_query_execution_stats(&mut result, started.elapsed(), queue_time);
        Ok(result)
    }

    fn is_retryable(&self, error: &Self::Error) -> bool {
        transient_object_store_error(error).is_some()
    }

    fn should_cache(&self, result: &Self::Output) -> bool {
        logs_result_is_cacheable(result, self.state.delete_requests.is_some())
    }

    fn merge(
        &self,
        request: &Self::Request,
        results: Vec<Self::Output>,
    ) -> Result<Self::Response, Self::Error> {
        Ok(merge_frontend_results(
            results,
            loki_direction(request.direction.as_deref())?,
            request.limit,
            request.interval,
            self.time_range.end_ns,
        ))
    }
}

fn planned_query_params(request: &QueryParams, range: TimeRange, partitioned: bool) -> QueryParams {
    let mut params = request.clone();
    params.time = None;
    params.since = None;
    params.start = Some(range.start_ns);
    params.end = Some(range.end_ns);
    if partitioned {
        params.interval = None;
    }
    params
}

fn logs_cache_key(
    tenant: &str,
    request: &QueryParams,
    range: TimeRange,
    bounds: FingerprintBounds,
    encoding: LokiStreamEncoding,
) -> CacheKey {
    CacheKey::new(format!(
        "logs\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
        tenant,
        request.query,
        range.start_ns,
        range.end_ns,
        request.step.unwrap_or_default(),
        request.interval.unwrap_or_default(),
        request.limit.unwrap_or_default(),
        request.direction.as_deref().unwrap_or_default(),
        bounds.min,
        bounds.max,
        encoding as u8,
    ))
}

fn logs_result_is_cacheable(result: &Value, delete_requests_configured: bool) -> bool {
    !delete_requests_configured
        && result
            .pointer("/data/result")
            .and_then(Value::as_array)
            .is_some_and(|results| !results.is_empty())
        && result.get("warnings").is_none()
}

pub(crate) fn split_ranges(range: TimeRange, split_ns: i64) -> Vec<TimeRange> {
    if range.start_ns == range.end_ns || split_ns <= 0 {
        return vec![range];
    }
    let mut ranges = Vec::new();
    let mut start = range.start_ns;
    while start < range.end_ns {
        let next_boundary = start
            .div_euclid(split_ns)
            .saturating_add(1)
            .saturating_mul(split_ns);
        let end = next_boundary.min(range.end_ns).max(start.saturating_add(1));
        ranges.push(TimeRange {
            start_ns: start,
            end_ns: end,
        });
        start = end;
    }
    ranges
}

pub(crate) fn stream_shards(
    state: &QuerierState,
    tenant: &str,
    query: &str,
    range: TimeRange,
    target_bytes: u64,
) -> Result<Vec<FingerprintBounds>, HttpQueryError> {
    let Ok(query) = parse_query(query) else {
        return Ok(vec![full_fingerprint_bounds()]);
    };
    let plan = plan_stream_query(tenant, range, query, &state.label_index, &state.block_index)?;
    Ok(fingerprint_shards(
        &plan.fingerprints,
        &plan.blocks,
        target_bytes,
    ))
}

pub(crate) async fn execute_index_shards_query(
    state: &QuerierState,
    security: &crate::RequestSecurity,
    headers: &crate::HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let raw_query = raw_query.ok_or(HttpQueryError::MissingQueryParameter("query"))?;
    let params = crate::parse_query_params(Some(raw_query))?;
    let target = url::form_urlencoded::parse(raw_query.as_bytes())
        .find(|(key, _)| key == "targetBytesPerShard")
        .map(|(_, value)| value.into_owned())
        .ok_or(HttpQueryError::MissingQueryParameter("targetBytesPerShard"))?;
    let target_bytes = target
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .or_else(|| {
            krabka_units::parse::positive_byte_size(&target)
                .ok()
                .map(ByteSizeExt::bytes_u64)
        });
    let target_bytes = target_bytes.ok_or_else(|| {
        HttpQueryError::LokiPlainParse("targetBytesPerShard must be a positive value".to_string())
    })?;
    let tenant =
        crate::authorized_tenant(state, security, headers, crate::TenantErrorSurface::Read).await?;
    let start = params
        .start
        .ok_or(HttpQueryError::MissingQueryParameter("start"))?;
    let end = params
        .end
        .ok_or(HttpQueryError::MissingQueryParameter("end"))?;
    let range = TimeRange::new(start, end)?;
    let state = state.with_tenant_limits(&tenant);
    crate::validate_query_range_limit(&state, range)?;
    crate::validate_query_string_bytes_limit(&state, &params.query)?;
    let state = state
        .with_request_tenant_index(tenant.as_str(), range)
        .await?;
    let shards = stream_shards(&state, tenant.as_str(), &params.query, range, target_bytes)?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query,
        source,
    })?;
    let plan = plan_stream_query(
        tenant.as_str(),
        range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    let shards = if plan.fingerprints.is_empty() {
        Value::Null
    } else {
        Value::Array(
            shards
                .into_iter()
                .map(|bounds| {
                    let fingerprints = plan
                        .fingerprints
                        .range(bounds.min..=bounds.max)
                        .copied()
                        .collect::<BTreeSet<_>>();
                    let blocks = plan
                        .blocks
                        .iter()
                        .filter(|block| !block.fingerprints.is_disjoint(&fingerprints))
                        .collect::<Vec<_>>();
                    let bytes = blocks.iter().fold(0_u64, |bytes, block| {
                        bytes.saturating_add(block.size.bytes_u64())
                    });
                    json!({
                        "bounds": {"min": bounds.min, "max": bounds.max},
                        "stats": {
                            "streams": fingerprints.len(),
                            "chunks": blocks.len(),
                            "entries": 0,
                            "bytes": bytes,
                        }
                    })
                })
                .collect(),
        )
    };
    Ok(json!({
        "shards": shards,
        "statistics": crate::loki_query_stats(),
        "chunkGroups": null,
    }))
}

pub(crate) fn fingerprint_shards(
    fingerprints: &BTreeSet<SeriesFingerprint>,
    blocks: &[BlockDescriptor],
    target_bytes: u64,
) -> Vec<FingerprintBounds> {
    if fingerprints.is_empty() || target_bytes == 0 {
        return vec![full_fingerprint_bounds()];
    }
    let mut weights = BTreeMap::<u64, u64>::new();
    for block in blocks {
        let matched = block
            .fingerprints
            .intersection(fingerprints)
            .copied()
            .collect::<Vec<_>>();
        let divisor = u64::try_from(matched.len()).unwrap_or(u64::MAX).max(1);
        let weight = block.size.bytes_u64().div_ceil(divisor);
        for fingerprint in matched {
            *weights.entry(fingerprint).or_default() = weights
                .get(&fingerprint)
                .copied()
                .unwrap_or_default()
                .saturating_add(weight);
        }
    }
    let ordered = fingerprints.iter().copied().collect::<Vec<_>>();
    let mut shards = Vec::new();
    let mut min = 0;
    let mut bytes = 0_u64;
    for (index, fingerprint) in ordered.iter().copied().enumerate() {
        bytes = bytes.saturating_add(weights.get(&fingerprint).copied().unwrap_or_default());
        if bytes >= target_bytes && index + 1 < ordered.len() {
            shards.push(FingerprintBounds {
                min,
                max: fingerprint,
            });
            min = fingerprint.saturating_add(1);
            bytes = 0;
        }
    }
    shards.push(FingerprintBounds { min, max: u64::MAX });
    shards
}

pub(crate) fn full_fingerprint_bounds() -> FingerprintBounds {
    FingerprintBounds {
        min: 0,
        max: u64::MAX,
    }
}

fn state_for_bounds(state: &QuerierState, tenant: &str, bounds: FingerprintBounds) -> QuerierState {
    if bounds == full_fingerprint_bounds() {
        return state.clone();
    }
    let mut label_index = LabelIndex::default();
    for (fingerprint, labels) in state.label_index.tenant_series(tenant) {
        if (bounds.min..=bounds.max).contains(&fingerprint) {
            label_index.insert_series(tenant, labels);
        }
    }
    let mut state = state.clone();
    state.label_index = label_index;
    state.dynamic_index = None;
    state
}

pub(crate) fn merge_frontend_results(
    results: Vec<Value>,
    direction: LokiDirection,
    limit: Option<usize>,
    interval: Option<i64>,
    end_exclusive: i64,
) -> Value {
    let mut results = results.into_iter();
    let Some(mut merged) = results.next() else {
        return json!({"status":"success","data":{"resultType":"streams","result":[]}});
    };
    for source in results {
        merge_one_result(&mut merged, source);
    }
    normalize_merged_series(&mut merged);
    let mut merged =
        apply_loki_stream_options(merged, direction, None, interval, Some(end_exclusive));
    apply_global_stream_limit(&mut merged, direction, limit);
    deduplicate_warnings(&mut merged);
    merged
}

fn merge_one_result(target: &mut Value, mut source: Value) {
    let source_results = source["data"]["result"].as_array_mut().map(std::mem::take);
    if let (Some(source_results), Some(target_results)) =
        (source_results, target["data"]["result"].as_array_mut())
    {
        target_results.extend(source_results);
    }
    if let Some(source_stats) = source.pointer("/data/stats") {
        merge_loki_query_stats(&mut target["data"]["stats"], source_stats);
    }
    if let Some(source_warnings) = source["warnings"].as_array_mut().map(std::mem::take) {
        target
            .as_object_mut()
            .expect("Loki response is an object")
            .entry("warnings")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .expect("Loki warnings are an array")
            .extend(source_warnings);
    }
}

fn normalize_merged_series(value: &mut Value) {
    let Some(result_type) = value.pointer("/data/resultType").and_then(Value::as_str) else {
        return;
    };
    let label_field = if result_type == "streams" {
        "stream"
    } else {
        "metric"
    };
    let value_field = "values";
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let mut grouped = BTreeMap::<String, Value>::new();
    for mut series in std::mem::take(results) {
        let key = serde_json::to_string(&series[label_field]).unwrap_or_default();
        if let Some(existing) = grouped.get_mut(&key) {
            if let Some(values) = series[value_field].as_array_mut().map(std::mem::take) {
                existing[value_field]
                    .as_array_mut()
                    .expect("Loki series values are an array")
                    .extend(values);
            }
        } else {
            grouped.insert(key, series);
        }
    }
    for series in grouped.values_mut() {
        if let Some(values) = series[value_field].as_array_mut() {
            values.sort_by(compare_entry_timestamps);
            values.dedup();
        }
    }
    results.extend(grouped.into_values());
}

fn entry_timestamp(entry: &Value) -> i64 {
    entry
        .as_array()
        .and_then(|entry| entry.first())
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

fn compare_entry_timestamps(left: &Value, right: &Value) -> std::cmp::Ordering {
    let left = left.as_array().and_then(|entry| entry.first());
    let right = right.as_array().and_then(|entry| entry.first());
    match (
        left.and_then(Value::as_str)
            .and_then(|value| value.parse::<i64>().ok()),
        right
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<i64>().ok()),
    ) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => left
            .and_then(Value::as_f64)
            .partial_cmp(&right.and_then(Value::as_f64))
            .unwrap_or(std::cmp::Ordering::Equal),
    }
}

fn apply_global_stream_limit(value: &mut Value, direction: LokiDirection, limit: Option<usize>) {
    let Some(limit) = limit else {
        return;
    };
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("streams") {
        return;
    }
    let Some(streams) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let mut entries = Vec::new();
    for (stream_index, stream) in streams.iter().enumerate() {
        if let Some(values) = stream["values"].as_array() {
            entries.extend(
                values.iter().enumerate().map(|(entry_index, entry)| {
                    (entry_timestamp(entry), stream_index, entry_index)
                }),
            );
        }
    }
    entries.sort_by_key(|(timestamp, stream, entry)| (*timestamp, *stream, *entry));
    if matches!(direction, LokiDirection::Backward) {
        entries.reverse();
    }
    let selected = entries
        .into_iter()
        .take(limit)
        .map(|(_, stream, entry)| (stream, entry))
        .collect::<BTreeSet<_>>();
    for (stream_index, stream) in streams.iter_mut().enumerate() {
        if let Some(values) = stream["values"].as_array_mut() {
            let mut entry_index = 0;
            values.retain(|_| {
                let keep = selected.contains(&(stream_index, entry_index));
                entry_index += 1;
                keep
            });
        }
    }
    streams.retain(|stream| {
        stream["values"]
            .as_array()
            .is_some_and(|values| !values.is_empty())
    });
}

fn deduplicate_warnings(value: &mut Value) {
    let keep = if let Some(warnings) = value["warnings"].as_array_mut() {
        let mut seen = BTreeSet::new();
        warnings.retain(|warning| seen.insert(serde_json::to_string(warning).unwrap_or_default()));
        !warnings.is_empty()
    } else {
        false
    };
    if !keep {
        value
            .as_object_mut()
            .expect("Loki response is an object")
            .remove("warnings");
    }
}

#[cfg(test)]
mod tests;
