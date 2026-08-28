//! axum HTTP surface for the query-frontend.
//!
//! It covers the Tempo query endpoints, tenant extraction, the v2 by-id
//! `status`/`message` envelope, and time-param parsing that matches the
//! querier's contract. `start` and `end` are epoch **seconds**, and a
//! fractional part is allowed.
//!
//! The router is generic over the backend and catalog pair. Tests therefore
//! drive `MockQuerier` with `MockCatalog`, and production binds `HttpQuerier`
//! with `TraceIndexCatalog`.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::json;

use crate::frontend::{
    QueryFrontend,
    backend::{BackendError, QuerierBackend},
    job::BlockCatalog,
    merge::TraceStatus,
    wire::parse_hex16,
};

const TENANT_HEADER: &str = "x-scope-orgid";

/// Render a propagated backend failure as the client response.
///
/// This keeps the upstream querier's status code and error text. An invalid
/// `TraceQL` query therefore surfaces as the querier's `4xx` body, not as a
/// silent empty `200`.
fn backend_error_response(err: &BackendError) -> Response {
    let (status, body) = err.to_http();
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    (code, body).into_response()
}

/// Build the query-frontend router for any backend/catalog pair.
pub fn router_with_backend<B, C>(qf: Arc<QueryFrontend<B, C>>) -> Router
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    Router::new()
        .route("/api/echo", get(echo))
        .route("/ready", get(ready))
        .route("/status", get(ready))
        .route("/api/search", get(search::<B, C>))
        .route("/api/v2/traces/{trace_id}", get(trace_by_id::<B, C>))
        .route("/api/v2/search/tags", get(search_tags_v2::<B, C>))
        .route(
            "/api/v2/search/tag/{tag}/values",
            get(search_tag_values_v2::<B, C>),
        )
        .route("/api/metrics/query_range", get(query_range::<B, C>))
        .route("/api/metrics/query", get(query_instant::<B, C>))
        .with_state(qf)
}

async fn echo() -> &'static str {
    "echo"
}

async fn ready() -> &'static str {
    "ready"
}

async fn search<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let query = match search_query(&uri) {
        Ok(Some(q)) => q,
        Ok(None) => return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response(),
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let (start_ns, end_ns) = match required_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let limit = bounded_count(&uri, "limit", qf.default_limit());
    let spss = bounded_count(&uri, "spss", qf.default_spss());

    match qf
        .search(&tenant, &query, start_ns, end_ns, limit, spss)
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}

async fn trace_by_id<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    if trace_id.len() != 32 || hex::decode(&trace_id).is_err() {
        return (StatusCode::BAD_REQUEST, "trace id must be 32 hex chars").into_response();
    }
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let tid = parse_hex16(&trace_id);
    let (trace, _metrics, status) = match qf.trace_by_id(&tenant, tid, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };

    let Some(trace) = trace else {
        return (StatusCode::NOT_FOUND, "trace not found").into_response();
    };
    // v2 envelope: { trace, status, message }. Per the querier's contract the
    // by-id endpoint does NOT carry a metrics block.
    let message = match status {
        TraceStatus::Partial => "trace exceeds max size; returned partially".to_string(),
        TraceStatus::Complete => String::new(),
    };
    Json(json!({
        "trace": trace.trace,
        "status": status.as_str(),
        "message": message,
    }))
    .into_response()
}

async fn search_tags_v2<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let scope = match scope_param(&uri) {
        Ok(scope) => scope,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let (tags, _metrics) = match qf.tag_names(&tenant, scope, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    let scopes: Vec<_> = tags
        .iter()
        .map(|st| json!({ "name": scope_name(st.scope), "tags": &st.tags }))
        .collect();
    Json(json!({ "scopes": scopes, "metrics": { "inspectedBytes": "0" } })).into_response()
}

async fn search_tag_values_v2<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Path(tag): Path<String>,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let (values, _metrics) = match qf.tag_values(&tenant, &tag, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    let tag_values: Vec<_> = values
        .iter()
        .map(|v| json!({ "type": &v.type_, "value": &v.value }))
        .collect();
    Json(json!({ "tagValues": tag_values, "metrics": { "inspectedBytes": "0" } })).into_response()
}

async fn query_range<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let Some(query) = metrics_query_param(&uri) else {
        return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
    };
    let (start_ns, end_ns) = match required_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let step_ns = match required_step(&uri) {
        Ok(step) => step,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let exemplar_limit = exemplar_limit(&uri);
    match qf
        .metrics_query(
            &tenant,
            &query,
            (start_ns, end_ns, step_ns),
            false,
            exemplar_limit,
        )
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}

async fn query_instant<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let Some(query) = metrics_query_param(&uri) else {
        return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
    };
    // Instant query: a window via start/end, else a single `time` point.
    let (start_ns, end_ns) =
        if query_param(&uri, "start").is_some() || query_param(&uri, "end").is_some() {
            match required_time_bounds(&uri) {
                Ok(bounds) => bounds,
                Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
            }
        } else {
            let ts = match optional_seconds(&uri, "time") {
                Ok(value) => value.unwrap_or(0),
                Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
            };
            (ts, ts)
        };
    let step_ns = end_ns.saturating_sub(start_ns).max(1);
    let exemplar_limit = exemplar_limit(&uri);
    match qf
        .metrics_query(
            &tenant,
            &query,
            (start_ns, end_ns, step_ns),
            true,
            exemplar_limit,
        )
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}

// --- param helpers (mirror the querier's contract) --------------------------

fn tenant(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .unwrap_or("anonymous")
        .to_string()
}

fn query_param(uri: &Uri, key: &str) -> Option<String> {
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find_map(|(k, v)| (k == key).then(|| v.into_owned()))
}

/// The `TraceQL` metrics query string.
///
/// Tempo accepts both `q` and `query` on the metrics endpoints. The Explore
/// `TraceQL` editor and the HTTP API send `q`. The Grafana Tempo datasource
/// that powers the Traces Drilldown app sends `query`. This accepts either, and
/// prefers `q`.
fn metrics_query_param(uri: &Uri) -> Option<String> {
    query_param(uri, "q").or_else(|| query_param(uri, "query"))
}

/// `q` (`TraceQL`) or the legacy `tags` logfmt form.
fn search_query(uri: &Uri) -> Result<Option<String>, &'static str> {
    if let Some(q) = query_param(uri, "q") {
        return Ok(Some(q));
    }
    query_param(uri, "tags")
        .map(|tags| tags_to_traceql(&tags).ok_or("invalid query parameter tags"))
        .transpose()
}

fn tags_to_traceql(tags: &str) -> Option<String> {
    let parts: Vec<String> = parse_logfmt_tags(tags)?
        .into_iter()
        .map(|(key, value)| {
            // The key is interpolated unquoted as a TraceQL attribute reference,
            // so a key carrying TraceQL-significant characters would inject query
            // structure (the value is already quoted+escaped). Reject such keys.
            key_is_safe_attribute(&key).then(|| {
                let field = if key.contains(':') {
                    key
                } else {
                    format!(".{}", key.strip_prefix('.').unwrap_or(&key))
                };
                let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
                format!("{field} = \"{escaped}\"")
            })
        })
        .collect::<Option<Vec<String>>>()?;
    (!parts.is_empty()).then(|| format!("{{ {} }}", parts.join(" && ")))
}

/// A legacy `tags=` key is a safe `TraceQL` attribute reference only if it is
/// made of identifier characters: alphanumerics plus `._:-`.
///
/// Any other character, such as `{`, `}`, `"`, `\`, `|`, `&`, `=` or
/// whitespace, could inject query structure once it is interpolated unquoted
/// into the generated `TraceQL`. This stays in sync with the querier's
/// `key_is_safe_attribute`.
fn key_is_safe_attribute(key: &str) -> bool {
    !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'))
}

fn parse_logfmt_tags(tags: &str) -> Option<Vec<(String, String)>> {
    let mut out = Vec::new();
    let mut rest = tags.trim_start();
    while !rest.is_empty() {
        let key_end = rest.find('=')?;
        let key = &rest[..key_end];
        if key.is_empty() || key.chars().any(char::is_whitespace) {
            return None;
        }
        rest = &rest[key_end + 1..];
        let (value, consumed) = parse_logfmt_value(rest)?;
        out.push((key.to_string(), value));
        rest = rest[consumed..].trim_start();
    }
    Some(out)
}

fn parse_logfmt_value(input: &str) -> Option<(String, usize)> {
    if let Some(input) = input.strip_prefix('"') {
        let mut value = String::new();
        let mut escaped = false;
        for (idx, ch) in input.char_indices() {
            if escaped {
                value.push(match ch {
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    other => other,
                });
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                return Some((value, idx + 2));
            } else {
                value.push(ch);
            }
        }
        return None;
    }
    let end = input
        .char_indices()
        .find_map(|(idx, ch)| ch.is_whitespace().then_some(idx))
        .unwrap_or(input.len());
    Some((input[..end].to_string(), end))
}

fn bounded_count(uri: &Uri, key: &str, default: usize) -> usize {
    query_param(uri, key)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn required_time_bounds(uri: &Uri) -> Result<(i64, i64), String> {
    let start_ns = required_seconds(uri, "start")?;
    let end_ns = required_seconds(uri, "end")?;
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok((start_ns, end_ns))
}

fn optional_time_bounds(uri: &Uri) -> Result<(i64, i64), String> {
    let start_ns = optional_seconds(uri, "start")?.unwrap_or(0);
    let end_ns = optional_seconds(uri, "end")?.unwrap_or(i64::MAX);
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok((start_ns, end_ns))
}

fn required_seconds(uri: &Uri, key: &str) -> Result<i64, String> {
    let Some(value) = query_param(uri, key) else {
        return Err(format!("missing query parameter {key}"));
    };
    parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
}

fn optional_seconds(uri: &Uri, key: &str) -> Result<Option<i64>, String> {
    query_param(uri, key)
        .map(|value| {
            parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
        })
        .transpose()
}

fn required_step(uri: &Uri) -> Result<i64, String> {
    let Some(value) = query_param(uri, "step") else {
        return Err("missing query parameter step".to_string());
    };
    let step = parse_step_to_ns(&value).ok_or("invalid step")?;
    if step <= 0 {
        return Err("step must be positive".to_string());
    }
    Ok(step)
}

/// `step` may be bare epoch-seconds OR a Go-duration such as `30s`, `5m` or
/// `100ms`. Grafana's Tempo datasource sends the duration form.
///
/// This mirrors the querier's `parse_step_to_ns`, so the frontend accepts
/// exactly what the querier accepts. Without it, the frontend would `400` a
/// query the querier handles.
fn parse_step_to_ns(value: &str) -> Option<i64> {
    parse_seconds_to_ns(value).or_else(|| i64::try_from(parse_go_duration_ns(value).ok()?).ok())
}

/// Parse a Go-style duration to nanoseconds.
///
/// Accepted forms are `1h`, `5m`, `30s`, `100ms`, `1m30s`, and a fractional
/// form such as `1.5s`. This stays in sync with the querier's parser.
fn parse_go_duration_ns(value: &str) -> Result<u64, String> {
    if value.is_empty() {
        return Err("empty duration".into());
    }
    let mut total = 0_u128;
    let mut rest = value;
    while !rest.is_empty() {
        let number_len = rest
            .char_indices()
            .take_while(|(_, c)| c.is_ascii_digit() || *c == '.')
            .map(|(idx, c)| idx + c.len_utf8())
            .last()
            .ok_or_else(|| format!("expected number in {value:?}"))?;
        let (number, tail) = rest.split_at(number_len);
        let unit_len = tail
            .char_indices()
            .take_while(|(_, c)| c.is_ascii_alphabetic() || *c == 'µ')
            .map(|(idx, c)| idx + c.len_utf8())
            .last()
            .ok_or_else(|| format!("expected unit after {number:?}"))?;
        let (unit, next) = tail.split_at(unit_len);
        let multiplier: u128 = match unit {
            "ns" => 1,
            "us" | "µs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(format!("unsupported unit {unit:?}")),
        };
        total = total
            .checked_add(parse_duration_component_ns(number, multiplier)?)
            .ok_or_else(|| "duration out of range".to_string())?;
        rest = next;
    }
    u64::try_from(total).map_err(|_| "duration out of range".into())
}

fn parse_duration_component_ns(number: &str, multiplier: u128) -> Result<u128, String> {
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty() && fraction.is_empty() {
        return Err(format!("invalid number {number:?}"));
    }
    if fraction.contains('.') {
        return Err(format!("invalid number {number:?}"));
    }
    let whole = if whole.is_empty() {
        0
    } else {
        whole
            .parse::<u128>()
            .map_err(|_| format!("invalid number {number:?}"))?
    };
    let whole_ns = whole
        .checked_mul(multiplier)
        .ok_or_else(|| "duration out of range".to_string())?;
    if fraction.is_empty() {
        return Ok(whole_ns);
    }
    let fraction_value = fraction
        .parse::<u128>()
        .map_err(|_| format!("invalid number {number:?}"))?;
    let scale = (0..fraction.len())
        .try_fold(1_u128, |acc, _| acc.checked_mul(10))
        .ok_or_else(|| "duration out of range".to_string())?;
    let fraction_ns = fraction_value
        .checked_mul(multiplier)
        .ok_or_else(|| "duration out of range".to_string())?
        / scale;
    whole_ns
        .checked_add(fraction_ns)
        .ok_or_else(|| "duration out of range".to_string())
}

fn parse_seconds_to_ns(value: &str) -> Option<i64> {
    let (negative, value) = value
        .strip_prefix('-')
        .map_or((false, value), |rest| (true, rest));
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > 9
    {
        return None;
    }
    let whole_ns = whole.parse::<i64>().ok()?.checked_mul(1_000_000_000)?;
    let fraction_ns = if fraction.is_empty() {
        0
    } else {
        format!("{fraction:0<9}").parse::<i64>().ok()?
    };
    let ns = whole_ns.checked_add(fraction_ns)?;
    if negative { ns.checked_neg() } else { Some(ns) }
}

fn exemplar_limit(uri: &Uri) -> Option<usize> {
    match query_param(uri, "exemplars").as_deref() {
        Some("false" | "0") => Some(0),
        Some("true") | None => None,
        Some(value) => value.parse().ok().or(None),
    }
}

fn scope_param(uri: &Uri) -> Result<Option<crabka_traceql::TagScope>, &'static str> {
    query_param(uri, "scope")
        .map(|s| parse_scope(&s).ok_or("invalid scope"))
        .transpose()
}

fn parse_scope(name: &str) -> Option<crabka_traceql::TagScope> {
    Some(match name {
        "resource" => crabka_traceql::TagScope::Resource,
        "span" => crabka_traceql::TagScope::Span,
        "intrinsic" => crabka_traceql::TagScope::Intrinsic,
        "event" => crabka_traceql::TagScope::Event,
        "link" => crabka_traceql::TagScope::Link,
        "instrumentation" => crabka_traceql::TagScope::Instrumentation,
        _ => return None,
    })
}

fn scope_name(scope: crabka_traceql::TagScope) -> &'static str {
    match scope {
        crabka_traceql::TagScope::Resource => "resource",
        crabka_traceql::TagScope::Span => "span",
        crabka_traceql::TagScope::Intrinsic => "intrinsic",
        crabka_traceql::TagScope::Event => "event",
        crabka_traceql::TagScope::Link => "link",
        crabka_traceql::TagScope::Instrumentation => "instrumentation",
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// The frontend's query parameters each have a boundary that only one
    /// input distinguishes: an empty window is legal but an inverted one is
    /// not, and a step must be strictly positive rather than merely parsed.
    #[test]
    fn frontend_time_bounds_and_step_reject_only_what_they_should() {
        let uri = |query: &str| {
            format!("http://x/api?{query}")
                .parse::<Uri>()
                .expect("a valid uri")
        };

        // end == start is an empty window and allowed, so `<` must not become
        // `<=`; end < start is refused, so it must not become `==` either.
        check!(super::optional_time_bounds(&uri("start=5&end=5")).is_ok());
        check!(super::optional_time_bounds(&uri("start=5&end=4")).is_err());
        check!(super::optional_time_bounds(&uri("start=5&end=6")).is_ok());
        check!(super::optional_time_bounds(&uri("")) == Ok((0, i64::MAX)));

        // A step is required, must parse, and must be strictly positive.
        check!(super::required_step(&uri("step=5s")) == Ok(5_000_000_000));
        check!(
            super::required_step(&uri("step=0")).is_err(),
            "zero is not positive"
        );
        check!(
            super::required_step(&uri("step=-1s")).is_err(),
            "nor is a negative step"
        );
        check!(
            super::required_step(&uri("")).is_err(),
            "and it is not optional"
        );
        check!(super::required_step(&uri("step=abc")).is_err());

        // The scope is optional, but an unrecognised name is refused rather
        // than quietly treated as absent.
        check!(super::scope_param(&uri("scope=span")) == Ok(Some(crabka_traceql::TagScope::Span)));
        check!(
            super::scope_param(&uri("scope=resource"))
                == Ok(Some(crabka_traceql::TagScope::Resource))
        );
        check!(super::scope_param(&uri("")) == Ok(None));
        check!(super::scope_param(&uri("scope=nonsense")).is_err());
    }

    /// `parse_duration_component_ns` scales one number by its unit. The
    /// fraction is divided by ten to its own length, so a two-digit fraction
    /// is hundredths -- which only shows when the fraction's length and its
    /// value differ, hence the ".05" case beside the ".5" one.
    #[test]
    fn a_duration_component_scales_its_fraction_by_length() {
        let parse = super::parse_duration_component_ns;
        let second = 1_000_000_000_u128;

        check!(parse("1", second) == Ok(second));
        check!(parse("2", second) == Ok(2 * second));
        check!(parse("0", second) == Ok(0));

        // The fraction divides by ten raised to its own length.
        check!(
            parse("1.5", second) == Ok(1_500_000_000),
            "one digit is tenths"
        );
        check!(
            parse("1.05", second) == Ok(1_050_000_000),
            "two digits are hundredths"
        );
        check!(
            parse("1.50", second) == Ok(1_500_000_000),
            "a trailing zero changes nothing"
        );
        check!(
            parse(".5", second) == Ok(500_000_000),
            "no whole part is still a number"
        );
        check!(parse("1.", second) == Ok(second), "no fraction either");

        // The multiplier is applied to both halves.
        check!(
            parse("1.5", 1_000) == Ok(1_500),
            "a microsecond scales the same way"
        );

        // What is not a number.
        check!(parse(".", second).is_err(), "a bare point has no digits");
        check!(parse("", second).is_err());
        // Two points is an error, and the same error either way: the split
        // takes the first point, so the second lands in the fraction and fails
        // to parse with the message the explicit check would have given.
        check!(
            parse("1.2.3", second) == Err(r#"invalid number "1.2.3""#.to_string()),
            "named, not merely refused"
        );
        check!(parse("a", second).is_err());
        check!(parse("-1", second).is_err(), "a component is unsigned");

        // A value too large to scale is refused rather than wrapping.
        check!(
            parse(&u128::MAX.to_string(), second).is_err(),
            "out of range"
        );
    }

    /// `parse_logfmt_value` returns the value and how many bytes it consumed.
    /// The length is what the caller resumes from, so every case checks it as
    /// well as the text -- a quoted value's length has to cover both quotes,
    /// which the value itself cannot show.
    #[test]
    fn a_logfmt_value_reports_what_it_consumed() {
        let parse = super::parse_logfmt_value;

        // Bare values run to the first whitespace.
        check!(parse("abc") == Some(("abc".to_string(), 3)));
        check!(
            parse("abc def") == Some(("abc".to_string(), 3)),
            "stops at the space"
        );
        check!(
            parse("") == Some((String::new(), 0)),
            "an empty value consumes nothing"
        );

        // Quoted values consume their quotes: two more than the text.
        check!(parse(r#""abc""#) == Some(("abc".to_string(), 5)));
        check!(
            parse(r#""abc" rest"#) == Some(("abc".to_string(), 5)),
            "and stop at the close"
        );
        check!(
            parse(r#""a b""#) == Some(("a b".to_string(), 5)),
            "whitespace inside quotes"
        );
        check!(
            parse(r#""""#) == Some((String::new(), 2)),
            "an empty quoted value is two bytes"
        );

        // Escapes: the three named ones become control characters, and anything
        // else after a backslash is itself.
        check!(
            parse(r#""a\nb""#) == Some(("a\nb".to_string(), 6)),
            "backslash-n is a newline"
        );
        check!(
            parse(r#""a\tb""#) == Some(("a\tb".to_string(), 6)),
            "backslash-t is a tab"
        );
        check!(
            parse(r#""a\rb""#) == Some(("a\rb".to_string(), 6)),
            "backslash-r is a return"
        );
        check!(
            parse(r#""a\"b""#) == Some((r#"a"b"#.to_string(), 6)),
            "an escaped quote"
        );
        check!(
            parse(r#""a\\b""#) == Some((r"a\b".to_string(), 6)),
            "an escaped backslash"
        );
        check!(
            parse(r#""a\qb""#) == Some(("aqb".to_string(), 6)),
            "an unknown escape is itself"
        );

        // An unterminated quote is not a value at all.
        check!(parse(r#""abc"#) == None);
        check!(parse(r#"""#) == None);
    }

    /// Go-style durations concatenate a number and a unit, and several pairs
    /// add up. Each unit is checked against the nanoseconds it stands for,
    /// since a table of multipliers is exactly where one wrong power of ten
    /// hides.
    #[test]
    fn go_durations_sum_their_components() {
        let parse = super::parse_go_duration_ns;

        check!(parse("1ns").unwrap() == 1);
        check!(parse("1us").unwrap() == 1_000);
        check!(
            parse("1µs").unwrap() == 1_000,
            "the micro sign is accepted too"
        );
        check!(parse("1ms").unwrap() == 1_000_000);
        check!(parse("1s").unwrap() == 1_000_000_000);
        check!(parse("1m").unwrap() == 60_000_000_000);
        check!(parse("1h").unwrap() == 3_600_000_000_000);

        // Several components add rather than replace one another.
        check!(parse("1h30m").unwrap() == 5_400_000_000_000);
        check!(parse("1m1s1ms").unwrap() == 61_001_000_000);
        check!(parse("0s").unwrap() == 0);

        // A fractional component scales by its unit.
        check!(parse("1.5s").unwrap() == 1_500_000_000);

        check!(parse("").is_err(), "an empty duration is not zero");
        check!(parse("10").is_err(), "a number with no unit");
        check!(parse("s").is_err(), "a unit with no number");
        check!(parse("1d").is_err(), "days are not a Go duration unit");
        check!(parse("1x").is_err(), "nor is anything else");
    }

    /// Seconds arrive as a decimal string and become whole nanoseconds. The
    /// fraction is padded rather than parsed as written, so "1.5" is half a
    /// second and not five nanoseconds.
    #[test]
    fn decimal_seconds_become_nanoseconds() {
        let parse = super::parse_seconds_to_ns;

        check!(parse("0").unwrap() == 0);
        check!(parse("1").unwrap() == 1_000_000_000);
        check!(
            parse("1.5").unwrap() == 1_500_000_000,
            "the fraction is padded, not read raw"
        );
        check!(
            parse("0.000000001").unwrap() == 1,
            "nine places is the smallest step"
        );
        check!(parse("1.000000001").unwrap() == 1_000_000_001);
        check!(
            parse("-1.5").unwrap() == -1_500_000_000,
            "the sign applies to the whole value"
        );
        check!(parse("-0").unwrap() == 0);

        check!(parse("").is_none(), "an empty value is not zero");
        check!(parse(".5").is_none(), "the whole part is required");
        check!(
            parse("1.").unwrap() == 1_000_000_000,
            "an empty fraction is none"
        );
        check!(parse("1.0000000001").is_none(), "past nanosecond precision");
        check!(parse("1.2.3").is_none(), "only one point");
        check!(parse("abc").is_none());
        check!(parse("1e9").is_none(), "no exponent form");
    }

    #[test]
    fn step_accepts_seconds_and_go_durations() {
        for (input, want) in [
            // Bare epoch-seconds (what the frontend already accepted).
            ("30", Some(30_000_000_000)),
            // Go-duration forms Grafana's Tempo datasource actually sends.
            ("30s", Some(30_000_000_000)),
            ("5m", Some(300_000_000_000)),
            ("1h", Some(3_600_000_000_000)),
            ("100ms", Some(100_000_000)),
            ("1m30s", Some(90_000_000_000)),
            // Garbage is still rejected.
            ("nonsense", None),
            ("30q", None),
        ] {
            check!(parse_step_to_ns(input) == want);
        }
    }

    #[test]
    fn tags_to_traceql_rejects_keys_with_metacharacters() {
        // Benign keys convert to a properly-quoted attribute match.
        assert2::assert!(tags_to_traceql("svc=b") == Some("{ .svc = \"b\" }".to_string()));
        assert2::assert!(
            tags_to_traceql("span:name=op") == Some("{ span:name = \"op\" }".to_string())
        );
        // A key carrying TraceQL-significant characters injects structure when
        // interpolated unquoted, so it is rejected.
        assert2::assert!(tags_to_traceql("a}=c").is_none());
        assert2::assert!(tags_to_traceql("a\"b=c").is_none());
        // The value side stays safely quoted even with metacharacters.
        assert2::assert!(
            tags_to_traceql("svc=a\"}||x") == Some("{ .svc = \"a\\\"}||x\" }".to_string())
        );
    }
}
