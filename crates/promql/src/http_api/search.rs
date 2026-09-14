use std::{cmp::Ordering, collections::BTreeMap, sync::Arc, time::SystemTime};

use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use krabka_blockstore::LabelMatcher;
use num_traits::ToPrimitive;
use serde_json::{Map, Value, json};
use url::form_urlencoded;

use super::{
    ApiError, Extension, Principal, PrometheusApiState, authorized_tenant_from_headers,
    selector_matchers, timestamp_ms,
};
use crate::MetricStore;

const DEFAULT_LIMIT: usize = 100;
const DEFAULT_BATCH_SIZE: usize = 100;
const MAX_BATCH_SIZE: usize = 10_000;
const MAX_SEARCH_TERMS: usize = 32;

#[derive(Clone, Copy)]
enum SearchKind {
    MetricNames,
    LabelNames,
    LabelValues,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FuzzAlgorithm {
    Subsequence,
    JaroWinkler,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortBy {
    Alpha,
    Score,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortDirection {
    Ascending,
    Descending,
}

struct SearchParams {
    searches: Vec<String>,
    matches: Vec<String>,
    label: Option<String>,
    case_sensitive: bool,
    fuzz_algorithm: FuzzAlgorithm,
    fuzz_threshold: f64,
    sort_by: SortBy,
    sort_direction: SortDirection,
    limit: usize,
    batch_size: usize,
    include_score: bool,
    include_metadata: bool,
    start_ms: i64,
    end_ms: i64,
}

struct SearchResult {
    value: String,
    score: f64,
}

pub(super) async fn search_metric_names<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    search(
        state,
        principal,
        headers,
        query.as_deref(),
        SearchKind::MetricNames,
    )
    .await
}

pub(super) async fn search_metric_names_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    search_post(
        state,
        principal,
        headers,
        query.as_deref(),
        &body,
        SearchKind::MetricNames,
    )
    .await
}

pub(super) async fn search_label_names<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    search(
        state,
        principal,
        headers,
        query.as_deref(),
        SearchKind::LabelNames,
    )
    .await
}

pub(super) async fn search_label_names_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    search_post(
        state,
        principal,
        headers,
        query.as_deref(),
        &body,
        SearchKind::LabelNames,
    )
    .await
}

pub(super) async fn search_label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    search(
        state,
        principal,
        headers,
        query.as_deref(),
        SearchKind::LabelValues,
    )
    .await
}

pub(super) async fn search_label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    search_post(
        state,
        principal,
        headers,
        query.as_deref(),
        &body,
        SearchKind::LabelValues,
    )
    .await
}

async fn search_post<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    principal: Principal,
    headers: HeaderMap,
    query: Option<&str>,
    body: &[u8],
    kind: SearchKind,
) -> Response {
    let mut encoded = query.unwrap_or_default().to_owned();
    if !encoded.is_empty() && !body.is_empty() {
        encoded.push('&');
    }
    encoded.push_str(&String::from_utf8_lossy(body));
    search(state, principal, headers, Some(&encoded), kind).await
}

async fn search<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    principal: Principal,
    headers: HeaderMap,
    query: Option<&str>,
    kind: SearchKind,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let params = match parse_search_params(query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    if matches!(kind, SearchKind::LabelValues) && params.label.as_ref().is_none_or(String::is_empty)
    {
        return ApiError::bad_data("label parameter is required").into_response();
    }

    let matcher_sets = match search_matchers(&params.matches) {
        Ok(matchers) => matchers,
        Err(error) => return error.into_response(),
    };
    let mut values = BTreeMap::new();
    for matchers in &matcher_sets {
        let found = match kind {
            SearchKind::MetricNames => {
                state
                    .store
                    .label_values(
                        tenant.as_str(),
                        "__name__",
                        matchers,
                        params.start_ms,
                        params.end_ms,
                    )
                    .await
            }
            SearchKind::LabelNames => {
                state
                    .store
                    .label_names(tenant.as_str(), matchers, params.start_ms, params.end_ms)
                    .await
            }
            SearchKind::LabelValues => {
                state
                    .store
                    .label_values(
                        tenant.as_str(),
                        params.label.as_deref().unwrap_or_default(),
                        matchers,
                        params.start_ms,
                        params.end_ms,
                    )
                    .await
            }
        };
        let found = match found {
            Ok(found) => found,
            Err(error) => return ApiError::from(error).into_response(),
        };
        for value in found {
            if let Some(score) = search_score(&value, &params) {
                values.entry(value).or_insert(score);
            }
        }
    }

    let mut results = values
        .into_iter()
        .map(|(value, score)| SearchResult { value, score })
        .collect::<Vec<_>>();
    results.sort_by(|left, right| match params.sort_by {
        SortBy::Alpha => left.value.cmp(&right.value),
        SortBy::Score => right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.value.cmp(&right.value)),
    });
    if params.sort_direction == SortDirection::Descending {
        results.reverse();
    }
    let has_more = params.limit > 0 && results.len() > params.limit;
    if params.limit > 0 {
        results.truncate(params.limit);
    }

    let metadata = if params.include_metadata && matches!(kind, SearchKind::MetricNames) {
        match state.store.metadata(tenant.as_str(), None).await {
            Ok(scan) => scan
                .metadata
                .into_iter()
                .map(|record| (record.metric_family_name.clone(), record))
                .collect(),
            Err(error) => return ApiError::from(error).into_response(),
        }
    } else {
        BTreeMap::new()
    };

    let mut output = String::new();
    for batch in results.chunks(params.batch_size) {
        let records = batch
            .iter()
            .map(|result| search_record(result, kind, &params, &metadata))
            .collect::<Vec<_>>();
        output.push_str(&json!({ "results": records }).to_string());
        output.push('\n');
    }
    output.push_str(&json!({ "status": "success", "has_more": has_more }).to_string());
    output.push('\n');

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/x-ndjson; charset=utf-8"),
            (
                header::HeaderName::from_static("x-mimir-response-streaming-enabled"),
                "true",
            ),
        ],
        output,
    )
        .into_response()
}

fn search_record(
    result: &SearchResult,
    kind: SearchKind,
    params: &SearchParams,
    metadata: &BTreeMap<String, crate::MetadataRecord>,
) -> Value {
    let field = match kind {
        SearchKind::MetricNames | SearchKind::LabelNames => "name",
        SearchKind::LabelValues => "value",
    };
    let mut record = Map::new();
    record.insert(field.into(), Value::String(result.value.clone()));
    if params.include_score {
        record.insert("score".into(), json!(result.score));
    }
    if let Some(metadata) = metadata.get(&result.value) {
        if !metadata.metric_type.is_empty() {
            record.insert("type".into(), Value::String(metadata.metric_type.clone()));
        }
        if !metadata.help.is_empty() {
            record.insert("help".into(), Value::String(metadata.help.clone()));
        }
        if !metadata.unit.is_empty() {
            record.insert("unit".into(), Value::String(metadata.unit.clone()));
        }
    }
    Value::Object(record)
}

fn parse_search_params(raw: Option<&str>) -> Result<SearchParams, ApiError> {
    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| ApiError::internal("system clock is before the Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| ApiError::internal("current timestamp exceeds i64"))?;
    let mut params = SearchParams {
        searches: Vec::new(),
        matches: Vec::new(),
        label: None,
        case_sensitive: true,
        fuzz_algorithm: FuzzAlgorithm::Subsequence,
        fuzz_threshold: 0.0,
        sort_by: SortBy::Alpha,
        sort_direction: SortDirection::Ascending,
        limit: DEFAULT_LIMIT,
        batch_size: DEFAULT_BATCH_SIZE,
        include_score: false,
        include_metadata: false,
        start_ms: now_ms - 3_600_000,
        end_ms: now_ms,
    };
    let mut explicit_sort_direction = false;
    for (name, value) in form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
        match name.as_ref() {
            "search[]" => params.searches.push(value.into_owned()),
            "match[]" => params.matches.push(value.into_owned()),
            "label" => params.label = Some(value.into_owned()),
            "case_sensitive" => params.case_sensitive = parse_bool(&value)?,
            "fuzz_alg" => {
                params.fuzz_algorithm = match value.as_ref() {
                    "subsequence" => FuzzAlgorithm::Subsequence,
                    "jarowinkler" => FuzzAlgorithm::JaroWinkler,
                    _ => return Err(ApiError::bad_data("invalid fuzz_alg parameter")),
                };
            }
            "fuzz_threshold" => {
                let threshold = value
                    .parse::<u8>()
                    .map_err(|_| ApiError::bad_data("invalid fuzz_threshold parameter"))?;
                if threshold > 100 {
                    return Err(ApiError::bad_data(
                        "fuzz_threshold must be between 0 and 100",
                    ));
                }
                params.fuzz_threshold = f64::from(threshold) / 100.0;
            }
            "sort_by" => {
                params.sort_by = match value.as_ref() {
                    "alpha" => SortBy::Alpha,
                    "score" => SortBy::Score,
                    _ => return Err(ApiError::bad_data("invalid sort_by parameter")),
                };
            }
            "sort_dir" => {
                explicit_sort_direction = true;
                params.sort_direction = match value.as_ref() {
                    "asc" => SortDirection::Ascending,
                    "dsc" | "desc" => SortDirection::Descending,
                    _ => return Err(ApiError::bad_data("invalid sort_dir parameter")),
                };
            }
            "limit" => params.limit = parse_usize(&value, "limit")?,
            "batch_size" => {
                params.batch_size = parse_usize(&value, "batch_size")?;
                if params.batch_size == 0 {
                    params.batch_size = DEFAULT_BATCH_SIZE;
                }
                if params.batch_size > MAX_BATCH_SIZE {
                    return Err(ApiError::bad_data("batch_size must not exceed 10000"));
                }
            }
            "include_score" => params.include_score = parse_bool(&value)?,
            "include_metadata" => params.include_metadata = parse_bool(&value)?,
            "start" => params.start_ms = timestamp_ms(&value)?,
            "end" => params.end_ms = timestamp_ms(&value)?,
            _ => {}
        }
    }
    if params.searches.len() > MAX_SEARCH_TERMS {
        return Err(ApiError::bad_data("too many search terms; maximum is 32"));
    }
    if params.sort_by == SortBy::Score && params.searches.is_empty() {
        return Err(ApiError::bad_data("sort_by=score requires a search term"));
    }
    if params.sort_by == SortBy::Score && explicit_sort_direction {
        return Err(ApiError::bad_data(
            "sort_dir cannot be used with sort_by=score",
        ));
    }
    if params.end_ms < params.start_ms {
        return Err(ApiError::bad_data(
            "end timestamp must not be before start time",
        ));
    }
    Ok(params)
}

fn parse_usize(value: &str, name: &str) -> Result<usize, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::bad_data(format!("invalid {name} parameter")))
}

fn parse_bool(value: &str) -> Result<bool, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::bad_data("invalid boolean parameter"))
}

fn search_matchers(selectors: &[String]) -> Result<Vec<Vec<LabelMatcher>>, ApiError> {
    if selectors.is_empty() {
        return Ok(vec![Vec::new()]);
    }
    let mut matcher_sets = Vec::new();
    for selector in selectors {
        matcher_sets.extend(selector_matchers(selector).map_err(ApiError::from)?);
    }
    Ok(matcher_sets)
}

fn search_score(value: &str, params: &SearchParams) -> Option<f64> {
    if params.searches.is_empty() {
        return Some(0.0);
    }
    params
        .searches
        .iter()
        .filter_map(|term| score_one(value, term, params))
        .max_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal))
}

fn score_one(value: &str, term: &str, params: &SearchParams) -> Option<f64> {
    let (value, term) = if params.case_sensitive {
        (value.to_owned(), term.to_owned())
    } else {
        (value.to_lowercase(), term.to_lowercase())
    };
    if term.is_empty() {
        return Some(1.0);
    }
    let score = match params.fuzz_algorithm {
        FuzzAlgorithm::Subsequence => subsequence_score(&value, &term)?,
        FuzzAlgorithm::JaroWinkler => {
            if value.starts_with(&term) {
                1.0
            } else if let Some(index) = value.find(&term) {
                let max_index = value.len().saturating_sub(term.len()).max(1);
                1.0 - 0.9 * as_f64(index) / as_f64(max_index)
            } else if params.fuzz_threshold > 0.0 {
                jaro_winkler(&value, &term)
            } else {
                return None;
            }
        }
    };
    (score >= params.fuzz_threshold).then_some(score)
}

fn subsequence_score(value: &str, term: &str) -> Option<f64> {
    if value.starts_with(term) {
        return Some(1.0);
    }
    let value = value.chars().collect::<Vec<_>>();
    let mut position = 0;
    let mut first = None;
    let mut last = 0;
    for wanted in term.chars() {
        let offset = value[position..]
            .iter()
            .position(|candidate| *candidate == wanted)?;
        position += offset;
        first.get_or_insert(position);
        last = position;
        position += 1;
    }
    let span = last + 1 - first.unwrap_or_default();
    Some(as_f64(term.chars().count()) / as_f64(span))
}

fn jaro_winkler(left: &str, right: &str) -> f64 {
    let left = left.chars().collect::<Vec<_>>();
    let right = right.chars().collect::<Vec<_>>();
    if left == right {
        return 1.0;
    }
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let distance = left
        .len()
        .max(right.len())
        .saturating_div(2)
        .saturating_sub(1);
    let mut left_match = vec![false; left.len()];
    let mut right_match = vec![false; right.len()];
    let mut matches = 0;
    for (index, character) in left.iter().enumerate() {
        let start = index.saturating_sub(distance);
        let end = (index + distance + 1).min(right.len());
        if let Some(other) =
            (start..end).find(|other| !right_match[*other] && right[*other] == *character)
        {
            left_match[index] = true;
            right_match[other] = true;
            matches += 1;
        }
    }
    if matches == 0 {
        return 0.0;
    }
    let transpositions = left
        .iter()
        .enumerate()
        .filter(|(index, _)| left_match[*index])
        .zip(
            right
                .iter()
                .enumerate()
                .filter(|(index, _)| right_match[*index]),
        )
        .filter(|((_, left), (_, right))| left != right)
        .count()
        / 2;
    let matches = as_f64(matches);
    let jaro = (matches / as_f64(left.len())
        + matches / as_f64(right.len())
        + (matches - as_f64(transpositions)) / matches)
        / 3.0;
    let prefix = left
        .iter()
        .zip(&right)
        .take_while(|(left, right)| left == right)
        .count()
        .min(4);
    let prefix = as_f64(prefix);
    jaro + prefix * 0.1 * (1.0 - jaro)
}

fn as_f64(value: usize) -> f64 {
    value
        .to_f64()
        .expect("string length is representable as f64")
}
