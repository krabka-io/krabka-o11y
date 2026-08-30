use super::*;

#[test]
pub(crate) fn native_header_detection_requires_native_log_shape() {
    for (key, value, want) in [
        ("krabka-wal-record-type", Some(&b"log-line"[..]), true),
        ("krabka-log-timestamp-ns", Some(&b"1"[..]), true),
        ("krabka-log-label-app", Some(&b"api"[..]), true),
        ("krabka-wal-record-type", Some(&b"log"[..]), false),
        ("other", None, false),
    ] {
        let header = KafkaWalHeader {
            key: key.to_string(),
            value: value.map(<[u8]>::to_vec),
        };
        assert_eq!(has_native_kafka_log_headers(&[header]), want);
    }
}
