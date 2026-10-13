use super::*;

struct NativeHeaderCase {
    key: &'static str,
    value: Option<&'static [u8]>,
    expected: bool,
}

#[test]
pub(crate) fn native_header_detection_requires_native_log_shape() {
    for case in [
        NativeHeaderCase {
            key: "krabka-wal-record-type",
            value: Some(&b"log-line"[..]),
            expected: true,
        },
        NativeHeaderCase {
            key: "krabka-log-timestamp-ns",
            value: Some(&b"1"[..]),
            expected: true,
        },
        NativeHeaderCase {
            key: "krabka-log-label-app",
            value: Some(&b"api"[..]),
            expected: true,
        },
        NativeHeaderCase {
            key: "krabka-wal-record-type",
            value: Some(&b"log"[..]),
            expected: false,
        },
        NativeHeaderCase {
            key: "other",
            value: None,
            expected: false,
        },
    ] {
        let header = KafkaWalHeader {
            key: case.key.to_string(),
            value: case.value.map(<[u8]>::to_vec),
        };
        assert_eq!(has_native_kafka_log_headers(&[header]), case.expected);
    }
}
