use super::*;

/// Tempo `TraceByIDResponse`, which is
/// `message TraceByIDResponse { Trace trace = 1; }`. It is the
/// `/api/v2/traces/{id}` protobuf body.
///
/// Grafana's Tempo datasource decodes the v2 trace-by-id response into this
/// message. The inner `Trace` is wire-identical to OTLP `TracesData`, so this
/// type models the field as `TracesData`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct TraceByIdResponse {
    #[prost(message, optional, tag = "1")]
    pub(crate) trace: Option<OtlpTracesData>,
}
