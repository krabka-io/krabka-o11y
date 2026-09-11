use super::{DistributorState, Principal, TracesError, decode_jaeger_thrift, produce_spans};

pub(crate) async fn handle_jaeger_compact_datagram(
    state: &DistributorState,
    body: &[u8],
) -> Result<(), TracesError> {
    // A datagram has no header, so it names no tenant. The policy decides:
    // the anonymous policy stores it as the anonymous tenant, and a policy
    // that requires a tenant rejects it.
    //
    // A datagram also carries no credential. The receiver runs only when
    // authentication is off, and then every request is unauthenticated.
    let tenant = state.resolve_tenant(&Principal::Unauthenticated, None)?;
    let spans = decode_jaeger_thrift(body)?;
    state.enforce_ingest(&tenant, &spans)?;
    produce_spans(state.sink.as_ref(), &tenant, spans).await
}
