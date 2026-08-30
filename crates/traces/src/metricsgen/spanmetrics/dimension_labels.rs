use super::{SpanRecord, sorted_labels, span_kind_dim, status_dim};

/// Dimension labels for the Tempo-compatible RED series identity.
#[must_use]
pub fn dimension_labels(span: &SpanRecord) -> Vec<(String, String)> {
    sorted_labels(vec![
        ("service".to_string(), span.service_name.clone()),
        ("span_name".to_string(), span.name.clone()),
        (
            "span_kind".to_string(),
            span_kind_dim(span.kind).to_string(),
        ),
        (
            "status_code".to_string(),
            status_dim(span.status).to_string(),
        ),
    ])
}
