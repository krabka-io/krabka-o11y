use super::*;

/// One OTLP span.
///
/// This type extracts `span_id` for dedup and keeps the whole span in `rest`,
/// so serialization re-emits the querier's exact span shape.
///
/// GAP5 is confirmed correct and is not a bug. The by-id span key is `spanId`,
/// **base64**-encoded. That is the standard OTLP protobuf-JSON byte-field
/// encoding the querier's `trace_json` emits. Search results (`SpanJson`)
/// instead key on `spanID` in **hex**, which is Tempo's search shape.
///
/// The two are different Tempo response formats, and each pipeline is
/// internally consistent: by-id is base64 end-to-end and search is hex
/// end-to-end. The respective dedup keys therefore never mix encodings. No
/// conversion is needed here, and none would be correct.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OtlpSpanJson {
    #[serde(rename = "spanId", default)]
    pub span_id: String,
    #[serde(flatten)]
    pub rest: serde_json::Map<String, serde_json::Value>,
}
