use super::{DistributorState, TracesError, decode_jaeger_thrift, produce_spans};

pub(crate) async fn handle_jaeger_compact_datagram(
    state: &DistributorState,
    tenant: &str,
    body: &[u8],
) -> Result<(), TracesError> {
    let spans = decode_jaeger_thrift(body)?;
    state.enforce_ingest(tenant, &spans)?;
    produce_spans(state.sink.as_ref(), tenant, spans).await
}
