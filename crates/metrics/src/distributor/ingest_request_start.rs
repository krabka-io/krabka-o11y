use std::future::Future;

use tracing::Instrument as _;

use super::{
    BodyBytes, ByteSize, ByteSizeExt as _, DistributorState, HeaderMap, IngestRequest,
    IntoResponse, Principal, PushError, PushOutcome, RequestOutcome, Response, StdDurationExt as _,
    TenantAccessError, TenantId, Time, WAL_TOPIC, authorized_tenant_from_headers,
};

/// What an ingest handler knows before it decodes a request: when the request
/// started, how large its body is, and the tenant it is authorized for.
pub(crate) struct IngestRequestStart {
    pub(crate) started: std::time::Instant,
    pub(crate) body_size: ByteSize,
    tenant: Result<TenantId, TenantAccessError>,
}

impl IngestRequestStart {
    /// Starts the request's clock and authorizes its tenant.
    pub(crate) fn begin(headers: &HeaderMap, principal: &Principal, body: &BodyBytes) -> Self {
        Self {
            started: std::time::Instant::now(),
            body_size: ByteSize::from_bytes(body.len() as u64),
            tenant: authorized_tenant_from_headers(headers, principal),
        }
    }

    /// The tenant the request is authorized for, or why it is not.
    pub(crate) fn authorized_tenant(&self) -> Result<&TenantId, TenantAccessError> {
        self.tenant.as_ref().map_err(Clone::clone)
    }

    /// The request's one ingest span.
    pub(crate) fn span(&self) -> tracing::Span {
        ingest_span(self.tenant.as_ref().ok(), self.body_size)
    }

    /// Runs one format-specific push under the request's ingest span.
    pub(crate) async fn run<'a, Outcome, Operation, OperationFuture>(
        &'a self,
        state: &'a DistributorState,
        headers: &'a HeaderMap,
        body: &'a [u8],
        operation: Operation,
    ) -> Result<Outcome, PushError>
    where
        Operation:
            FnOnce(&'a DistributorState, &'a TenantId, &'a HeaderMap, &'a [u8]) -> OperationFuture,
        OperationFuture: Future<Output = Result<Outcome, PushError>>,
    {
        async { operation(state, self.authorized_tenant()?, headers, body).await }
            .instrument(self.span())
            .await
    }

    /// Records the outcome of a push that counts its decoded series, and
    /// answers with the push's response.
    pub(crate) fn respond(
        &self,
        state: &DistributorState,
        result: Result<super::PushOutcome, PushError>,
    ) -> Response {
        record_ingest_outcome(
            state,
            &result,
            self.body_size,
            self.started.elapsed().as_time(),
        );
        match result {
            Ok(outcome) => outcome.success.into_response(),
            Err(error) => error.into_response(),
        }
    }
}

/// Builds the per-request ingest span. This function declares
/// `krabka.ingest.series` empty, and `push_inner` records it after it decodes
/// the request body.
///
/// `tenant` is the resolved tenant, or `None` when the request names no usable
/// tenant. The span of such a request carries the tenant `unknown`. The label
/// is for the span only, and the push path rejects the request.
pub(crate) fn ingest_span(tenant: Option<&TenantId>, body_size: ByteSize) -> tracing::Span {
    let tenant = tenant.map_or("unknown", TenantId::as_str);
    tracing::info_span!(
        "metrics_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = WAL_TOPIC,
        krabka.tenant = %tenant,
        krabka.ingest.series = tracing::field::Empty,
        krabka.ingest.bytes = body_size.bytes_u64(),
    )
}

/// Records an ingest request outcome on the distributor metrics bundle, if one
/// is configured. `body_size` is the compressed request-body length. `items` is
/// the decoded series count on success and `0` on error.
pub(crate) fn record_ingest_outcome(
    state: &DistributorState,
    result: &Result<PushOutcome, PushError>,
    body_size: ByteSize,
    elapsed: Time,
) {
    let Some(metrics) = &state.metrics else {
        return;
    };
    metrics.record_ingest(IngestRequest {
        outcome: RequestOutcome::from_result(result),
        body: body_size,
        items: result.as_ref().map_or(0, |outcome| outcome.items),
        elapsed,
    });
}
