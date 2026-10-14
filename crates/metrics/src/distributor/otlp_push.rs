use prost::Message as _;

use super::*;

super::ingest_push_handler!(otlp_push, otlp_push_inner, respond_otlp_push);

fn respond_otlp_push(
    state: &DistributorState,
    ingest: &IngestRequestStart,
    result: Result<OtlpPushOutcome, PushError>,
) -> Response {
    if let Some(metrics) = &state.metrics {
        metrics.record_ingest(IngestRequest {
            outcome: RequestOutcome::from_result(&result),
            body: ingest.body_size,
            items: result.as_ref().map_or(0, |outcome| outcome.items),
            elapsed: ingest.started.elapsed().as_time(),
        });
    }
    match result {
        Ok(outcome) => {
            let mut response = outcome.success.into_response();
            let body = ExportMetricsServiceResponse {
                partial_success: outcome.partial_success.map(|partial| {
                    ExportMetricsPartialSuccess {
                        rejected_data_points: i64::try_from(partial.rejected_data_points)
                            .unwrap_or(i64::MAX),
                        error_message: partial.error_message,
                    }
                }),
            }
            .encode_to_vec();
            *response.body_mut() = axum::body::Body::from(body.clone());
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            );
            response.headers_mut().insert(
                axum::http::header::CONTENT_LENGTH,
                HeaderValue::from_str(&body.len().to_string())
                    .expect("a response body length is an HTTP header value"),
            );
            response.headers_mut().insert(
                "x-content-type-options",
                HeaderValue::from_static("nosniff"),
            );
            response
        }
        Err(error) => error.into_response(),
    }
}
