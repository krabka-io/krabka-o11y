use super::{Serialize, Deserialize, TraceEnvelopeJson, ByteSize, ByteSizeExt};

/// The querier's v2 by-id response body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TraceByIdResponseJson {
    #[serde(default)]
    pub trace: TraceEnvelopeJson,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub message: String,
}

impl TraceByIdResponseJson {
    /// Total spans across all resource/scope groups.
    #[must_use]
    pub fn span_count(&self) -> usize {
        self.trace
            .resource_spans
            .iter()
            .flat_map(|rs| rs.scope_spans.iter())
            .map(|ss| ss.spans.len())
            .sum()
    }

    /// Cheap size estimate of the assembled trace: the serialized length.
    #[must_use]
    pub fn approx_size(&self) -> ByteSize {
        serde_json::to_vec(&self.trace).map_or(<ByteSize as ByteSizeExt>::ZERO, |v| {
            ByteSize::from_bytes(u64::try_from(v.len()).unwrap_or(u64::MAX))
        })
    }

    /// True when this body carries no spans. A querier that did not hold the
    /// trace returns an empty or `None` body, which this type models as no
    /// resourceSpans.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.span_count() == 0
    }
}
