use super::{DecodedExemplar, Labels, OtlpExemplar, ToPrimitive, bytes_to_hex, insert_attributes, nanos_to_millis, otlp_exemplar};

pub(crate) fn exemplar(exemplar: &OtlpExemplar) -> Option<DecodedExemplar> {
    let value = match exemplar.value {
        Some(otlp_exemplar::Value::AsDouble(value)) => value,
        Some(otlp_exemplar::Value::AsInt(value)) => value.to_f64().unwrap_or(f64::MAX),
        None => return None,
    };
    let mut labels = Labels::new();
    insert_attributes(&mut labels, &exemplar.filtered_attributes);
    if !exemplar.trace_id.is_empty() {
        labels.insert("trace_id", bytes_to_hex(&exemplar.trace_id));
    }
    if !exemplar.span_id.is_empty() {
        labels.insert("span_id", bytes_to_hex(&exemplar.span_id));
    }
    Some(DecodedExemplar {
        labels,
        timestamp_ms: nanos_to_millis(exemplar.time_unix_nano),
        value,
    })
}
