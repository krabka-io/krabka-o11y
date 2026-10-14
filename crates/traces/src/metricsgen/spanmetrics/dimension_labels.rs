use super::{SpanRecord, sorted_labels, span_kind_dim, status_dim};

/// Dimension labels for the Tempo-compatible RED series identity.
#[must_use]
pub fn dimension_labels(span: &SpanRecord) -> Vec<krabka_traceql::MetricLabel> {
    sorted_labels(red_dimension_pairs(span).into())
}

/// The four label pairs every span is aggregated by, in fixed order.
pub(crate) fn red_dimension_pairs(span: &SpanRecord) -> [krabka_traceql::MetricLabel; 4] {
    [
        krabka_traceql::MetricLabel::new("service", span.service_name.clone()),
        krabka_traceql::MetricLabel::new("span_name", span.name.clone()),
        krabka_traceql::MetricLabel::new("span_kind", span_kind_dim(span.kind)),
        krabka_traceql::MetricLabel::new("status_code", status_dim(span.status)),
    ]
}
