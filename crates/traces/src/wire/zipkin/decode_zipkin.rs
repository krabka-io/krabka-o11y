use super::*;

/// Decode a Zipkin v2 JSON span array.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_zipkin(body: &[u8]) -> Result<Vec<Span>, WireError> {
    let raw: Vec<ZipkinSpan> =
        serde_json::from_slice(body).map_err(|err| WireError::Decode(err.to_string()))?;
    let mut out = Vec::with_capacity(raw.len());

    for span in raw {
        let resource_attrs = span
            .local_endpoint
            .and_then(|endpoint| endpoint.service_name)
            .map(|service| {
                vec![KeyValue {
                    key: "service.name".into(),
                    value: AttrValue::Str(service),
                }]
            })
            .unwrap_or_default();
        let (status, status_message) = zipkin_status(&span.tags);
        let mut span_attrs = span
            .tags
            .into_iter()
            .map(|(key, value)| KeyValue {
                key,
                value: AttrValue::Str(value),
            })
            .collect::<Vec<_>>();
        if let Some(service) = span
            .remote_endpoint
            .and_then(|endpoint| endpoint.service_name)
        {
            span_attrs.push(KeyValue {
                key: "peer.service".into(),
                value: AttrValue::Str(service),
            });
        }
        let events = span
            .annotations
            .into_iter()
            .map(|annotation| crate::span::EventRecord {
                time_unix_nano: annotation.timestamp.saturating_mul(1_000),
                name: annotation.value,
                attrs: Vec::new(),
            })
            .collect();

        out.push(Span {
            trace_id: hex_fixed::<16>(&span.trace_id)?,
            span_id: hex_fixed::<8>(&span.id)?,
            parent_span_id: span.parent_id.as_deref().map(hex_fixed::<8>).transpose()?,
            name: span.name,
            kind: zipkin_kind(span.kind.as_deref()),
            start_ns: span.timestamp.saturating_mul(1_000),
            duration_ns: span.duration.saturating_mul(1_000),
            status,
            status_message,
            resource_attrs,
            span_attrs,
            events,
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        });
    }

    Ok(out)
}
