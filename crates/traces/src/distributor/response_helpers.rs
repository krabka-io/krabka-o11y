use axum::{body::Body, http::StatusCode, response::{IntoResponse, Response}};
use krabka_observability::service_metrics::IngestPushMeasurement;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse;
use prost::Message as _;

use super::{
    DistributorState, LimitError, Span, TenantId, TracesError, produce_spans,
    tempo_error_response,
};

pub(crate) fn error_response(err: &TracesError) -> Response {
    let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match err {
        TracesError::Limit(_) | TracesError::RateLimit(_) => {
            tempo_error_response(status, err.to_string())
        }
        // The same 403 body that every Krabka listener sends for a denial.
        TracesError::TenantDenied(denied) => denied.clone().into_response(),
        _ => Response::builder()
            .status(status)
            .body(Body::from(err.to_string()))
            .expect("the HTTP response builds"),
    }
}

pub(crate) fn limit_error_to_traces_error(err: &LimitError) -> TracesError {
    match err {
        LimitError::IngestionRateExceeded { .. } => TracesError::RateLimit(err.message()),
        LimitError::MaxSpansPerTrace { .. }
        | LimitError::MaxSpansPerRequest { .. }
        | LimitError::AttributeTooLong { .. }
        | LimitError::TracesPerSearchExceeded { .. }
        | LimitError::SearchDurationExceeded { .. } => TracesError::Limit(err.message()),
    }
}

pub(crate) async fn append_decoded_response(
    state: &DistributorState,
    tenant: &TenantId,
    spans: Vec<Span>,
    success: Response,
) -> Response {
    if let Err(err) = state.enforce_ingest(tenant, &spans) {
        return error_response(&err);
    }
    let accepted = spans.len() as u64;
    match produce_spans(state.sink.as_ref(), tenant, spans).await {
        Ok(()) => {
            // Attribute accepted spans to the tenant once per request (batch
            // size), not per span-record, keeping cardinality bounded.
            state.metrics.record_ingest_spans(tenant, accepted);
            success.into_response()
        }
        Err(err) => {
            // A produce failure is an actual WAL-append error (distinct from a
            // 4xx validation/rate-limit reject handled above).
            state.metrics.record_wal_append_failure();
            if let TracesError::ProduceBatch {
                appended, total, ..
            } = &err
            {
                state
                    .metrics
                    .wal_produce
                    .record_batch_failure(*appended, *total);
            }
            error_response(&err)
        }
    }
}

/// Record one push-handler ingest outcome from the response status, and return
/// the response unchanged.
///
/// The outcome is `status="ok"` for any 2xx. The produce error site bumps the
/// WAL/produce failure counter separately, so a 4xx validation or rate-limit
/// reject here does not inflate that counter.
pub(crate) fn record_ingest_response(
    state: &DistributorState,
    resp: Response,
    measurement: IngestPushMeasurement,
) -> Response {
    state
        .metrics
        .record_ingest(measurement.ingest_request(resp.status()));
    resp
}

pub(crate) fn otlp_success_response() -> Response {
    let body = ExportTraceServiceResponse {
        partial_success: None,
    }
    .encode_to_vec();
    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(body))
        .expect("the HTTP response builds")
}
