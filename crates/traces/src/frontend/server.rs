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

use std::{convert::Infallible, pin::Pin, sync::Arc, task::{Context, Poll}};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Extension, Path, State},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::Engine as _;
use futures::Stream;
use krabka_blockstore::TenantId;
use krabka_observability::{RoleReadiness, server_security::Principal};
use opentelemetry_proto::tonic::{
    common::v1::KeyValue,
    trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData},
};
use prost::Message as _;
use serde::{de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::{
    frontend::{
        QueryFrontend,
        backend::{BackendError, QuerierBackend},
        job::BlockCatalog,
        merge::TraceStatus,
        wire::{parse_hex16, SearchResponseJson, TraceByIdResponseJson},
    },
    limits::overrides_api_response,
    readiness::tempo_readiness_routes,
};

pub(crate) fn status_text_response(status: StatusCode, body: impl Into<String>) -> Response {
    Response::builder()
        .status(status)
        .body(Body::from(body.into()))
        .expect("the HTTP response builds")
}

pub(crate) fn status_bytes_response(
    status: StatusCode,
    content_type: &'static str,
    body: Vec<u8>,
) -> Response {
    Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .expect("the HTTP response builds")
}

fn query_time_range(bounds: &TenantTimeBounds) -> krabka_blockstore::TimeRange {
    krabka_blockstore::TimeRange {
        start_ns: bounds.start_ns,
        end_ns: bounds.end_ns,
    }
}

// --- param helpers (mirror the querier's contract) --------------------------

use axum::{extract::FromRequestParts, http::request::Parts};

use crate::frontend::MetricsQueryWindow;

use crate::tempo_query_routes::tempo_query_routes;

use std::collections::BTreeSet;


/// What every query-frontend read handler extracts from its request: the
/// frontend, the authenticated principal, the headers, and the URI.
pub(crate) struct FrontendRequest<B: QuerierBackend, C: BlockCatalog> {
    pub(crate) qf: Arc<QueryFrontend<B, C>>,
    pub(crate) principal: Principal,
    pub(crate) headers: HeaderMap,
    pub(crate) uri: Uri,
}

impl<B: QuerierBackend, C: BlockCatalog> FrontendRequest<B, C> {
    /// The request's tenant under the frontend's tenant policy.
    pub(crate) fn tenant_request(&self) -> TenantRequest<'_> {
        TenantRequest {
            headers: &self.headers,
            principal: &self.principal,
            policy: &self.qf.cfg.tenant_policy,
            uri: &self.uri,
        }
    }

    /// The tenant and optional time bounds used by read endpoints.
    pub(crate) fn tenant_and_optional_bounds(&self) -> Result<TenantTimeBounds, Box<Response>> {
        tenant_and_bounds(self.tenant_request(), optional_time_bounds)
    }
}

impl<B, C> FromRequestParts<Arc<QueryFrontend<B, C>>> for FrontendRequest<B, C>
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        qf: &Arc<QueryFrontend<B, C>>,
    ) -> Result<Self, Self::Rejection> {
        let Extension(principal) = Extension::<Principal>::from_request_parts(parts, qf)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Self {
            qf: Arc::clone(qf),
            principal,
            headers: parts.headers.clone(),
            uri: parts.uri.clone(),
        })
    }
}

/// `/api/metrics/query_range`, or for [`MetricsQueryKind::Instant`] the
/// single-sample `/api/metrics/query`.
pub(crate) async fn metrics_query<B, C, K>(request: FrontendRequest<B, C>) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
    K: RouteVariant<MetricsQueryKind>,
{
    let metrics = match metrics_request(request.tenant_request()) {
        Ok(metrics) => metrics,
        Err(rejection) => return *rejection,
    };
    let FrontendRequest { qf, uri, .. } = request;
    let instant = matches!(K::VARIANT, MetricsQueryKind::Instant);
    let bounds = if instant {
        crate::querier::http::instant_metric_bounds(&uri).map(|bounds| MetricsQueryWindow {
            start_ns: bounds.start_ns,
            end_ns: bounds.end_ns,
            step_ns: bounds.step_ns,
        })
    } else {
        required_time_bounds(&uri).and_then(|bounds| {
            required_step(&uri).map(|step_ns| MetricsQueryWindow {
                start_ns: bounds.start_ns,
                end_ns: bounds.end_ns,
                step_ns,
            })
        })
    };
    let bounds = match bounds {
        Ok(bounds) => bounds,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let exemplar_limit = exemplar_limit(&uri);
    let resp = match qf
        .metrics_query(
            &metrics.tenant,
            &metrics.query,
            bounds,
            instant,
            exemplar_limit,
        )
        .await
    {
        Ok(resp) => resp,
        Err(err) => return backend_error_response(&err),
    };
    if !instant {
        return Json(resp).into_response();
    }
    Json(crate::querier::http::instant_metrics_response(
        resp.series.iter().filter_map(|series| {
            series
                .samples
                .first()
                .map(|sample| crate::querier::http::InstantMetricSample {
                    labels: serde_json::to_value(&series.labels).expect("JSON metric labels"),
                    value: sample.value,
                })
        }),
    ))
    .into_response()
}

/// Build the query-frontend router for any backend/catalog pair.
///
/// `readiness` is what `/ready` and `/status` report. The frontend's own gates
/// include `querier-membership`, so a frontend with nobody to fan out to is
/// reported as unready rather than as a role that answers every query with a
/// transport error.
pub fn router_with_backend<B, C>(qf: Arc<QueryFrontend<B, C>>, readiness: RoleReadiness) -> Router
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    tempo_query_routes!(
        Router::new();
        echo = get(echo),
        buildinfo = get(buildinfo),
        overrides = get(overrides::<B, C>).post(overrides::<B, C>).patch(overrides::<B, C>).delete(overrides::<B, C>),
        search = get(search::<B, C, WholeSearch>),
        search_stream = get(search::<B, C, StreamedSearch>),
        trace_v1 = get(trace_by_id::<B, C, V1Api>),
        trace_v2 = get(trace_by_id::<B, C, V2Api>),
        tags_v1 = get(search_tags::<B, C, V1Api>),
        tags_v2 = get(search_tags::<B, C, V2Api>),
        tag_values_v1 = get(search_tag_values::<B, C, V1Api>),
        tag_values_v2 = get(search_tag_values::<B, C, V2Api>),
        metrics_range = get(metrics_query::<B, C, RangeMetrics>),
        metrics_instant = get(metrics_query::<B, C, InstantMetrics>),
    )
    .merge(tempo_readiness_routes(readiness))
    .with_state(qf)
}

/// `/api/search`, or for [`SearchDelivery::Streamed`] the NDJSON stream of
/// cumulative responses that `/api/search/stream` sends as shards complete.
pub(crate) async fn search<B, C, D>(request: FrontendRequest<B, C>) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
    D: RouteVariant<SearchDelivery>,
{
    let search = match search_request(request.tenant_request()) {
        Ok(search) => search,
        Err(rejection) => return *rejection,
    };
    let FrontendRequest { qf, uri, .. } = request;
    let limit = bounded_count(&uri, "limit", qf.default_limit());
    let spss = bounded_count(&uri, "spss", qf.default_spss());

    if matches!(D::VARIANT, SearchDelivery::Streamed) {
        return match qf
            .search_stream(
                &search.tenant,
                &search.query,
                search.bounds.start_ns,
                search.bounds.end_ns,
                limit,
                spss,
            )
            .await
        {
            Ok(receiver) => ndjson_search_stream(receiver),
            Err(error) => status_text_response(StatusCode::BAD_GATEWAY, error.to_string()),
        };
    }
    match qf
        .search(
            &search.tenant,
            &search.query,
            search.bounds.start_ns,
            search.bounds.end_ns,
            limit,
            spss,
        )
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}

/// `/api/search/tag/{tag}/values`, or for [`ApiVersion::V2`] the typed
/// `/api/v2/search/tag/{tag}/values`.
pub(crate) async fn search_tag_values<B, C, V>(
    request: FrontendRequest<B, C>,
    Path(tag): Path<String>,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
    V: RouteVariant<ApiVersion>,
{
    let bounds = match request.tenant_and_optional_bounds() {
        Ok(bounds) => bounds,
        Err(rejection) => return *rejection,
    };
    let qf = request.qf;
    let result = match qf
        .tag_values(&bounds.tenant, &tag, query_time_range(&bounds))
        .await
    {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    if matches!(V::VARIANT, ApiVersion::V1) {
        let tag_values = result
            .values
            .into_iter()
            .map(|value| value.value)
            .collect::<BTreeSet<_>>();
        return Json(json!({ "tagValues": tag_values })).into_response();
    }
    let tag_values: Vec<_> = result
        .values
        .iter()
        .map(|v| json!({ "type": &v.type_, "value": &v.value }))
        .collect();
    with_warnings(
        json!({ "tagValues": tag_values, "metrics": { "inspectedBytes": "0" } }),
        &result.warnings,
    )
}

/// `/api/search/tags`, or for [`ApiVersion::V2`] the scoped
/// `/api/v2/search/tags`.
pub(crate) async fn search_tags<B, C, V>(request: FrontendRequest<B, C>) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
    V: RouteVariant<ApiVersion>,
{
    let bounds = match request.tenant_and_optional_bounds() {
        Ok(bounds) => bounds,
        Err(rejection) => return *rejection,
    };
    let FrontendRequest { qf, uri, .. } = request;
    // The v1 endpoint lists every scope's names as one flat set.
    let scope = match V::VARIANT {
        ApiVersion::V1 => None,
        ApiVersion::V2 => match scope_param(&uri) {
            Ok(scope) => scope,
            Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
        },
    };
    let result = match qf
        .tag_names(&bounds.tenant, scope, query_time_range(&bounds))
        .await
    {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    if matches!(V::VARIANT, ApiVersion::V1) {
        let tag_names = result
            .tags
            .into_iter()
            .flat_map(|scope| scope.tags)
            .collect::<BTreeSet<_>>();
        return Json(json!({ "tagNames": tag_names })).into_response();
    }
    let scopes: Vec<_> = result
        .tags
        .iter()
        .map(|st| json!({ "name": scope_name(st.scope), "tags": &st.tags }))
        .collect();
    with_warnings(
        json!({ "scopes": scopes, "metrics": { "inspectedBytes": "0" } }),
        &result.warnings,
    )
}

/// The v2 trace-by-id envelope at `/api/v2/traces/{id}`, or for
/// [`ApiVersion::V1`] the bare trace that `/api/traces/{id}` answers.
pub(crate) async fn trace_by_id<B, C, V>(
    request: FrontendRequest<B, C>,
    Path(trace_id): Path<String>,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
    V: RouteVariant<ApiVersion>,
{
    if trace_id.len() != 32 || hex::decode(&trace_id).is_err() {
        return status_text_response(StatusCode::BAD_REQUEST, "trace id must be 32 hex chars");
    }
    let bounds = match tenant_and_bounds(request.tenant_request(), optional_time_bounds) {
        Ok(request) => request,
        Err(rejection) => return *rejection,
    };
    let FrontendRequest { qf, headers, .. } = request;
    let tid = parse_hex16(&trace_id);
    let result = match qf
        .trace_by_id(&bounds.tenant, tid, bounds.start_ns, bounds.end_ns)
        .await
    {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    if matches!(V::VARIANT, ApiVersion::V1) {
        return trace_v1_response(&headers, result.trace);
    }

    let Some(trace) = result.trace else {
        // 404 asserts the trace does not exist. With a querier out of the
        // fan-out, nobody asked the one that may have held it, so the honest
        // 404 says what was not looked at rather than claiming absence.
        if result.warnings.is_empty() {
            return status_text_response(StatusCode::NOT_FOUND, "trace not found");
        }
        return status_text_response(
            StatusCode::NOT_FOUND,
            format!(
                "trace not found in the queriers that answered; {}",
                result.warnings.join("; ")
            ),
        );
    };
    // v2 envelope: { trace, status, message }. Per the querier's contract the
    // by-id endpoint does NOT carry a metrics block. Tempo's own `message`
    // carries why a trace came back `PARTIAL`, so an excluded querier is
    // reported there rather than in a field Grafana would not read.
    let message = match result.status {
        TraceStatus::Partial => result.warnings.join("; "),
        TraceStatus::Complete => String::new(),
    };
    Json(json!({
        "trace": trace.trace,
        "status": result.status.as_str(),
        "message": message,
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// A step must be strictly positive rather than merely parsed. The time
    /// bounds are the querier's `optional_time_bounds`, which its own tests
    /// pin.
    #[test]
    fn frontend_step_rejects_only_what_it_should() {
        let uri = |query: &str| {
            format!("http://x/api?{query}")
                .parse::<Uri>()
                .expect("a valid uri")
        };

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
        check!(super::scope_param(&uri("scope=span")) == Ok(Some(krabka_traceql::TagScope::Span)));
        check!(
            super::scope_param(&uri("scope=resource"))
                == Ok(Some(krabka_traceql::TagScope::Resource))
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
        let parse = crate::querier::http::parse_duration_component_ns;
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
        let parse = crate::querier::http::parse_logfmt_value;
        let matches = |input: &str, value: &str, consumed: usize| {
            parse(input).is_some_and(|parsed| parsed.value == value && parsed.consumed == consumed)
        };

        // Bare values run to the first whitespace.
        check!(matches("abc", "abc", 3));
        check!(matches("abc def", "abc", 3), "stops at the space");
        check!(matches("", "", 0), "an empty value consumes nothing");

        // Quoted values consume their quotes: two more than the text.
        check!(matches(r#""abc""#, "abc", 5));
        check!(matches(r#""abc" rest"#, "abc", 5), "and stop at the close");
        check!(matches(r#""a b""#, "a b", 5), "whitespace inside quotes");
        check!(
            matches(r#""""#, "", 2),
            "an empty quoted value is two bytes"
        );

        // Escapes: the three named ones become control characters, and anything
        // else after a backslash is itself.
        check!(matches(r#""a\nb""#, "a\nb", 6), "backslash-n is a newline");
        check!(matches(r#""a\tb""#, "a\tb", 6), "backslash-t is a tab");
        check!(matches(r#""a\rb""#, "a\rb", 6), "backslash-r is a return");
        check!(matches(r#""a\"b""#, r#"a"b"#, 6), "an escaped quote");
        check!(matches(r#""a\\b""#, r"a\b", 6), "an escaped backslash");
        check!(
            matches(r#""a\qb""#, "aqb", 6),
            "an unknown escape is itself"
        );

        // An unterminated quote is not a value at all.
        check!(parse(r#""abc"#).is_none());
        check!(parse(r#"""#).is_none());
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

    #[yare::parameterized(
        bare_seconds = { "30", Some(30_000_000_000) },
        seconds = { "30s", Some(30_000_000_000) },
        minutes = { "5m", Some(300_000_000_000) },
        hours = { "1h", Some(3_600_000_000_000) },
        milliseconds = { "100ms", Some(100_000_000) },
        compound = { "1m30s", Some(90_000_000_000) },
        malformed_number = { "nonsense", None },
        malformed_unit = { "30q", None },
    )]
    fn step_accepts_seconds_and_go_durations(input: &str, expected: Option<i64>) {
        check!(parse_step_to_ns(input) == expected);
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
use krabka_traceql::key_is_safe_attribute;

use crate::querier::http::{
    TenantRequest, TenantTimeBounds, metrics_request, optional_time_bounds, parse_go_duration_ns,
    parse_logfmt_tags, parse_seconds_to_ns, query_param, request_tenant, tenant_and_bounds,
    wants_json,
};

/// Render a propagated backend failure as the client response.
///
/// This keeps the upstream querier's status code and error text. An invalid
/// `TraceQL` query therefore surfaces as the querier's `4xx` body, not as a
/// silent empty `200`.
pub(crate) fn backend_error_response(err: &BackendError) -> Response {
    let error = err.to_http();
    let code = StatusCode::from_u16(error.status).unwrap_or(StatusCode::BAD_GATEWAY);
    status_text_response(code, error.body)
}

pub(crate) fn bounded_count(uri: &Uri, key: &str, default: usize) -> usize {
    query_param(uri, key)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

pub(crate) async fn echo() -> &'static str {
    "echo"
}

pub(crate) fn exemplar_limit(uri: &Uri) -> Option<usize> {
    match query_param(uri, "exemplars").as_deref() {
        Some("false" | "0") => Some(0),
        Some("true") | None => None,
        Some(value) => value.parse().ok().or(None),
    }
}

pub(crate) fn parse_scope(name: &str) -> Option<krabka_traceql::TagScope> {
    Some(match name {
        "resource" => krabka_traceql::TagScope::Resource,
        "span" => krabka_traceql::TagScope::Span,
        "intrinsic" => krabka_traceql::TagScope::Intrinsic,
        "event" => krabka_traceql::TagScope::Event,
        "link" => krabka_traceql::TagScope::Link,
        "instrumentation" => krabka_traceql::TagScope::Instrumentation,
        _ => return None,
    })
}

/// `step` may be bare epoch-seconds OR a Go-duration such as `30s`, `5m` or
/// `100ms`. Grafana's Tempo datasource sends the duration form.
///
/// This mirrors the querier's `parse_step_to_ns`, so the frontend accepts
/// exactly what the querier accepts. Without it, the frontend would `400` a
/// query the querier handles.
pub(crate) fn parse_step_to_ns(value: &str) -> Option<i64> {
    parse_seconds_to_ns(value).or_else(|| i64::try_from(parse_go_duration_ns(value).ok()?).ok())
}

pub(crate) fn required_seconds(uri: &Uri, key: &str) -> Result<i64, String> {
    crate::querier::http::required_seconds_param(uri, key)
}

pub(crate) fn required_step(uri: &Uri) -> Result<i64, String> {
    let Some(value) = query_param(uri, "step") else {
        return Err("missing query parameter step".to_string());
    };
    let step = parse_step_to_ns(&value).ok_or("invalid step")?;
    if step <= 0 {
        return Err("step must be positive".to_string());
    }
    Ok(step)
}

pub(crate) fn scope_name(scope: krabka_traceql::TagScope) -> &'static str {
    match scope {
        krabka_traceql::TagScope::Resource => "resource",
        krabka_traceql::TagScope::Span => "span",
        krabka_traceql::TagScope::Intrinsic => "intrinsic",
        krabka_traceql::TagScope::Event => "event",
        krabka_traceql::TagScope::Link => "link",
        krabka_traceql::TagScope::Instrumentation => "instrumentation",
    }
}

pub(crate) fn scope_param(uri: &Uri) -> Result<Option<krabka_traceql::TagScope>, &'static str> {
    query_param(uri, "scope")
        .map(|s| parse_scope(&s).ok_or("invalid scope"))
        .transpose()
}

/// `q` (`TraceQL`) or the legacy `tags` logfmt form.
pub(crate) fn search_query(uri: &Uri) -> Result<Option<String>, &'static str> {
    if let Some(q) = query_param(uri, "q") {
        return Ok(Some(q));
    }
    query_param(uri, "tags")
        .map(|tags| tags_to_traceql(&tags).ok_or("invalid query parameter tags"))
        .transpose()
}

/// Answer with `body`, carrying `warnings` only when there are any, so a
/// complete answer stays the body Tempo returns.
pub(crate) fn with_warnings(mut body: serde_json::Value, warnings: &[String]) -> Response {
    if !warnings.is_empty() {
        body["warnings"] = json!(warnings);
    }
    Json(body).into_response()
}

pub(crate) async fn buildinfo() -> Response {
    Json(json!({
        "status": "success",
        "data": {
            "version": "3.0.3",
            "revision": "krabka",
            "branch": "main",
            "buildUser": "krabka",
            "buildDate": "",
            "goVersion": "",
        },
    }))
    .into_response()
}

pub(crate) async fn overrides<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    Extension(principal): Extension<Principal>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = match request_tenant(&headers, &principal, &qf.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    overrides_api_response(
        &qf.cfg.overrides,
        tenant.as_str(),
        &method,
        &headers,
        uri.query(),
        &body,
    )
}

pub(crate) fn tags_to_traceql(tags: &str) -> Option<String> {
    let parts: Vec<String> = parse_logfmt_tags(tags)?
        .into_iter()
        .map(|tag| {
            // The key is interpolated unquoted as a TraceQL attribute reference,
            // so a key carrying TraceQL-significant characters would inject query
            // structure (the value is already quoted+escaped). Reject such keys.
            key_is_safe_attribute(&tag.key).then(|| {
                let field = if tag.key.contains(':') {
                    tag.key
                } else {
                    format!(".{}", tag.key.strip_prefix('.').unwrap_or(&tag.key))
                };
                let escaped = tag.value.replace('\\', "\\\\").replace('"', "\\\"");
                format!("{field} = \"{escaped}\"")
            })
        })
        .collect::<Option<Vec<String>>>()?;
    (!parts.is_empty()).then(|| format!("{{ {} }}", parts.join(" && ")))
}



// Route variants, request parsing, and response encoders used by this surface.

/// A zero-sized type the router names to pick one variant of `E` for a
/// handler that several routes share.
pub(crate) trait RouteVariant<E> {
    const VARIANT: E;
}

/// The Tempo API version a route answers.
pub(crate) enum ApiVersion {
    V1,
    V2,
}

/// How `/api/search` delivers its result.
pub(crate) enum SearchDelivery {
    /// One JSON response once every shard completes.
    Whole,
    /// An NDJSON stream of cumulative responses as shards complete.
    Streamed,
}

/// Which `TraceQL` metrics query a route answers.
pub(crate) enum MetricsQueryKind {
    /// `/api/metrics/query_range`.
    Range,
    /// The single-sample `/api/metrics/query`.
    Instant,
}

/// Routes the Tempo v1 API.
pub(crate) struct V1Api;

impl RouteVariant<ApiVersion> for V1Api {
    const VARIANT: ApiVersion = ApiVersion::V1;
}

/// Routes the Tempo v2 API.
pub(crate) struct V2Api;

impl RouteVariant<ApiVersion> for V2Api {
    const VARIANT: ApiVersion = ApiVersion::V2;
}

/// Routes `/api/search`.
pub(crate) struct WholeSearch;

impl RouteVariant<SearchDelivery> for WholeSearch {
    const VARIANT: SearchDelivery = SearchDelivery::Whole;
}

/// Routes `/api/search/stream`.
pub(crate) struct StreamedSearch;

impl RouteVariant<SearchDelivery> for StreamedSearch {
    const VARIANT: SearchDelivery = SearchDelivery::Streamed;
}

/// Routes `/api/metrics/query_range`.
pub(crate) struct RangeMetrics;

impl RouteVariant<MetricsQueryKind> for RangeMetrics {
    const VARIANT: MetricsQueryKind = MetricsQueryKind::Range;
}

/// Routes `/api/metrics/query`.
pub(crate) struct InstantMetrics;

impl RouteVariant<MetricsQueryKind> for InstantMetrics {
    const VARIANT: MetricsQueryKind = MetricsQueryKind::Instant;
}


#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RequiredTimeBounds {
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

pub(crate) fn required_time_bounds(uri: &Uri) -> Result<RequiredTimeBounds, String> {
    let start_ns = required_seconds(uri, "start")?;
    let end_ns = required_seconds(uri, "end")?;
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok(RequiredTimeBounds { start_ns, end_ns })
}


/// The tenant, the `TraceQL` query, and the time window a search names, or
/// the response that rejects the search.
pub(crate) struct SearchRequest {
    pub(crate) tenant: TenantId,
    pub(crate) query: String,
    pub(crate) bounds: RequiredTimeBounds,
}

pub(crate) fn search_request(request: TenantRequest<'_>) -> Result<SearchRequest, Box<Response>> {
    let TenantRequest {
        headers,
        principal,
        policy,
        uri,
    } = request;
    let tenant = request_tenant(headers, principal, policy)?;
    let query = match search_query(uri) {
        Ok(Some(query)) => query,
        Ok(None) => {
            return Err(Box::new(status_text_response(
                StatusCode::BAD_REQUEST,
                "missing query parameter q",
            )));
        }
        Err(err) => return Err(Box::new(status_text_response(StatusCode::BAD_REQUEST, err))),
    };
    let bounds = required_time_bounds(uri)
        .map_err(|err| Box::new(status_text_response(StatusCode::BAD_REQUEST, err)))?;
    Ok(SearchRequest {
        tenant,
        query,
        bounds,
    })
}


/// The NDJSON body of `/api/search/stream`: one cumulative response per line,
/// or an `error` object for a shard that failed.
pub(crate) fn ndjson_search_stream(
    receiver: mpsc::Receiver<Result<SearchResponseJson, BackendError>>,
) -> Response {
    let body = NdjsonSearchStream { receiver };
    Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(body))
        .expect("the HTTP response builds")
}

struct NdjsonSearchStream {
    receiver: mpsc::Receiver<Result<SearchResponseJson, BackendError>>,
}

impl Stream for NdjsonSearchStream {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(item)) => item,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        };
        let value = match item {
            Ok(response) => serde_json::to_vec(&response).unwrap_or_default(),
            Err(error) => serde_json::to_vec(&serde_json::json!({ "error": error.to_string() }))
                .unwrap_or_default(),
        };
        let mut line = value;
        line.push(b'\n');
        Poll::Ready(Some(Ok(Bytes::from(line))))
    }
}


/// Tempo's v1 trace-by-id answer, for `/api/traces/{id}`.
///
/// Grafana's Tempo *backend* datasource fetches the trace-view there with
/// `Accept: application/protobuf` and proto-decodes the body as OTLP. The
/// answer therefore defaults to OTLP `TracesData` protobuf, which is Tempo's
/// v1 default. It falls back to the wrapped JSON for humans.
pub(crate) fn trace_v1_response(
    headers: &HeaderMap,
    trace: Option<TraceByIdResponseJson>,
) -> Response {
    let Some(trace) = trace else {
        return status_text_response(StatusCode::NOT_FOUND, "trace not found");
    };
    if wants_json(headers) {
        return Json(trace.trace).into_response();
    }
    match trace_protobuf(trace.trace) {
        Ok(bytes) => status_bytes_response(StatusCode::OK, "application/protobuf", bytes),
        Err(error) => status_text_response(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

fn trace_protobuf(trace: crate::frontend::wire::TraceEnvelopeJson) -> Result<Vec<u8>, String> {
    let mut value = serde_json::to_value(trace).map_err(|error| error.to_string())?;
    ids_to_hex(&mut value);
    let resources = take_array(&mut value, "resourceSpans")?;
    let trace = TracesData {
        resource_spans: resources
            .into_iter()
            .map(resource_from_json)
            .collect::<Result<_, _>>()?,
    };
    Ok(trace.encode_to_vec())
}

fn ids_to_hex(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for entry in object {
                let name = entry.0;
                let value = entry.1;
                if matches!(name.as_str(), "traceId" | "spanId" | "parentSpanId")
                    && let Some(encoded) = value.as_str()
                    && let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded)
                {
                    *value = serde_json::Value::String(hex::encode(bytes));
                } else {
                    ids_to_hex(value);
                }
            }
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(ids_to_hex),
        _ => {}
    }
}

fn take_array(value: &mut Value, key: &str) -> Result<Vec<Value>, String> {
    match value.as_object_mut().and_then(|value| value.remove(key)) {
        None => Ok(Vec::new()),
        Some(Value::Array(values)) => Ok(values),
        Some(_) => Err(format!("{key} must be an array")),
    }
}

fn take_attributes(value: &mut Value) -> Result<Vec<KeyValue>, String> {
    take_array(value, "attributes")?
        .into_iter()
        .map(|mut entry| {
            let raw = entry
                .as_object_mut()
                .ok_or("OTLP attribute must be an object")?
                .remove("value");
            let mut attribute: KeyValue = decode_json(entry)?;
            attribute.value = raw
                .as_ref()
                .map(crate::span::AttrValue::parse_otlp_json)
                .transpose()
                .map_err(|error| error.to_string())?;
            Ok(attribute)
        })
        .collect()
}

fn decode_json<T: DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|error| error.to_string())
}

fn resource_from_json(mut value: Value) -> Result<ResourceSpans, String> {
    let scopes = take_array(&mut value, "scopeSpans")?;
    let attributes = take_attributes(&mut value["resource"])?;
    let mut resource: ResourceSpans = decode_json(value)?;
    if let Some(resource) = &mut resource.resource {
        resource.attributes = attributes;
    }
    resource.scope_spans = scopes
        .into_iter()
        .map(scope_from_json)
        .collect::<Result<_, _>>()?;
    Ok(resource)
}

fn scope_from_json(mut value: Value) -> Result<ScopeSpans, String> {
    let spans = take_array(&mut value, "spans")?;
    let attributes = take_attributes(&mut value["scope"])?;
    let mut scope: ScopeSpans = decode_json(value)?;
    if let Some(scope) = &mut scope.scope {
        scope.attributes = attributes;
    }
    scope.spans = spans
        .into_iter()
        .map(span_from_json)
        .collect::<Result<_, _>>()?;
    Ok(scope)
}

fn span_from_json(mut value: Value) -> Result<Span, String> {
    let events = take_array(&mut value, "events")?;
    let links = take_array(&mut value, "links")?;
    let attributes = take_attributes(&mut value)?;
    let mut span: Span = decode_json(value)?;
    span.attributes = attributes;
    span.events = events
        .into_iter()
        .map(|mut event| {
            let attributes = take_attributes(&mut event)?;
            let mut event: opentelemetry_proto::tonic::trace::v1::span::Event = decode_json(event)?;
            event.attributes = attributes;
            Ok(event)
        })
        .collect::<Result<_, String>>()?;
    span.links = links
        .into_iter()
        .map(|mut link| {
            let attributes = take_attributes(&mut link)?;
            let mut link: opentelemetry_proto::tonic::trace::v1::span::Link = decode_json(link)?;
            link.attributes = attributes;
            Ok(link)
        })
        .collect::<Result<_, String>>()?;
    Ok(span)
}
