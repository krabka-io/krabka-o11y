use super::*;

pub(crate) fn exemplar(
    exemplar: &OtlpExemplar,
    strategy: TranslationStrategy,
) -> Option<DecodedExemplar> {
    let value = match exemplar.value {
        Some(otlp_exemplar::Value::AsDouble(value)) => value,
        Some(otlp_exemplar::Value::AsInt(value)) => value.to_f64().unwrap_or(f64::MAX),
        None => return None,
    };
    let mut labels = Labels::new();
    insert_attributes(&mut labels, &exemplar.filtered_attributes, strategy);
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

pub(crate) fn exemplar_belongs_to_bucket(
    value: f64,
    point: &HistogramDataPoint,
    bucket_idx: usize,
) -> bool {
    let lower_ok = bucket_idx
        .checked_sub(1)
        .and_then(|lower_idx| point.explicit_bounds.get(lower_idx))
        .is_none_or(|lower| value > *lower);
    let upper_ok = point
        .explicit_bounds
        .get(bucket_idx)
        .is_none_or(|upper| value <= *upper);
    lower_ok && upper_ok
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExemplarPolicy {
    Keep,
    Drop,
}

pub(crate) fn exemplars_for_bucket(
    exemplars: &[DecodedExemplar],
    point: &HistogramDataPoint,
    bucket_idx: usize,
) -> Vec<DecodedExemplar> {
    exemplars
        .iter()
        .filter(|exemplar| exemplar_belongs_to_bucket(exemplar.value, point, bucket_idx))
        .cloned()
        .collect()
}

pub(crate) fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    out
}
