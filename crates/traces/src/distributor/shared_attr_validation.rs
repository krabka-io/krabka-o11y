use std::collections::BTreeMap;

use super::{
    AttrValue, IngestEnforcer, KeyValue, Limits, MeasuredAttribute, Span, TracesError,
    limit_error_to_traces_error,
};

/// Pair an attribute key with the TRUE encoded byte length of its value.
///
/// This measures the real byte length rather than the length of a string form.
/// The `Limits::max_attribute` cap therefore sees the true size of `Bytes`,
/// `Int` and `Double` values. A `String` conversion would mis-report them: a
/// large byte blob would read as length 0 and would bypass the limit.
pub(crate) fn shared_attr_measured(attr: &KeyValue) -> MeasuredAttribute {
    let value_bytes = match &attr.value {
        AttrValue::Str(value) => value.len(),
        AttrValue::Bytes(value) => value.len(),
        AttrValue::Int(value) => value.to_le_bytes().len(),
        AttrValue::Double(value) => value.to_le_bytes().len(),
        AttrValue::Bool(_) => 1,
        value @ (AttrValue::Array(_) | AttrValue::Unsupported(_)) => {
            prost::Message::encoded_len(&value.otlp_value())
        }
    };
    MeasuredAttribute {
        key: attr.key.clone(),
        value_bytes: value_bytes as u64,
    }
}

pub(crate) fn check_shared_attrs(limits: &Limits, attrs: &[KeyValue]) -> Result<(), TracesError> {
    let flattened = attrs.iter().map(shared_attr_measured).collect::<Vec<_>>();
    IngestEnforcer::check_attributes(limits, &flattened)
        .map_err(|err| super::limit_error_to_traces_error(&err))
}

pub(crate) fn validate_shared(spans: &[Span], limits: &Limits) -> Result<(), TracesError> {
    IngestEnforcer::check_spans_per_request(limits, u64::try_from(spans.len()).unwrap_or(u64::MAX))
        .map_err(|err| limit_error_to_traces_error(&err))?;
    let mut spans_per_trace = BTreeMap::new();
    for span in spans {
        let count = spans_per_trace
            .entry(span.trace_id)
            .and_modify(|count| *count += 1_u64)
            .or_insert(1_u64);
        IngestEnforcer::check_trace_size(limits, *count)
            .map_err(|err| limit_error_to_traces_error(&err))?;
        check_shared_attrs(limits, &span.resource_attrs)?;
        check_shared_attrs(limits, &span.span_attrs)?;
        for event in &span.events {
            check_shared_attrs(limits, &event.attrs)?;
        }
        for link in &span.links {
            check_shared_attrs(limits, &link.attrs)?;
        }
    }
    Ok(())
}
