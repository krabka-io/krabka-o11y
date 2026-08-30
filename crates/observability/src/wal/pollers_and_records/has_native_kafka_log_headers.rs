use super::*;

pub(crate) fn has_native_kafka_log_headers(headers: &[KafkaWalHeader]) -> bool {
    headers.iter().any(|header| {
        header.key == "krabka-log-timestamp-ns"
            || header.key.starts_with("krabka-log-label-")
            || (header.key == "krabka-wal-record-type"
                && header
                    .value
                    .as_deref()
                    .is_some_and(|value| value == b"log-line"))
    })
}
