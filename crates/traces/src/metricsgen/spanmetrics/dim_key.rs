use super::*;

pub(crate) type DimKey = (String, String, String, String, Option<String>);

pub(crate) fn dim_key(span: &SpanRecord, include_status_message: bool) -> DimKey {
    (
        span.service_name.clone(),
        span.name.clone(),
        span_kind_dim(span.kind).to_string(),
        status_dim(span.status).to_string(),
        include_status_message.then(|| span.status_message.clone()),
    )
}
