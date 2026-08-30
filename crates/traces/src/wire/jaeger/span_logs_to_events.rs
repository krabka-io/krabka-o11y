use super::*;

pub(crate) fn span_logs_to_events(logs: &[JaegerLog]) -> Vec<crate::span::EventRecord> {
    logs.iter()
        .map(|log| {
            let name = log
                .fields
                .iter()
                .find_map(|field| {
                    if field.key != "event" {
                        return None;
                    }
                    match &field.value {
                        AttrValue::Str(value) if !value.is_empty() => Some(value.clone()),
                        _ => None,
                    }
                })
                .unwrap_or_else(|| "log".to_string());
            crate::span::EventRecord {
                time_unix_nano: log.timestamp_micros.saturating_mul(1_000),
                name,
                attrs: log.fields.clone(),
            }
        })
        .collect()
}
