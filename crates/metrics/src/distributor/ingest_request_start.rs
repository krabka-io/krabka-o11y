use std::future::Future;

use tracing::Instrument as _;

use super::{
    BodyBytes, ByteSize, ByteSizeExt as _, DistributorState, HeaderMap, IntoResponse, Principal,
    PushError, Response, StdDurationExt as _, TenantAccessError, TenantId,
    authorized_tenant_from_headers, ingest_span, record_ingest_outcome,
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
