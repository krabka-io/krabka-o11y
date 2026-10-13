use super::*;

struct ContentTypeCase {
    value: &'static str,
    accepted: Option<bool>,
}

#[test]
pub(crate) fn loki_content_type_and_body_decoding_accept_only_expected_forms() {
    let mut headers = HeaderMap::new();
    assert_eq!(decode_loki_http_body(&headers, b"raw").unwrap(), b"raw");
    headers.insert(CONTENT_ENCODING, "snappy".parse().unwrap());
    assert_eq!(decode_loki_http_body(&headers, b"raw").unwrap(), b"raw");
    headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut encoder, b"raw").unwrap();
    assert_eq!(
        decode_loki_http_body(&headers, &encoder.finish().unwrap()).unwrap(),
        b"raw"
    );
    headers.insert(CONTENT_ENCODING, "br".parse().unwrap());
    assert!(decode_loki_http_body(&headers, b"raw").is_err());

    for case in [
        ContentTypeCase {
            value: "application/json",
            accepted: Some(true),
        },
        ContentTypeCase {
            value: "Application/JSON; charset=utf-8",
            accepted: Some(true),
        },
        ContentTypeCase {
            value: "application/x-protobuf",
            accepted: Some(false),
        },
        ContentTypeCase {
            value: "application/json; charset",
            accepted: None,
        },
        ContentTypeCase {
            value: "application/json; charset=",
            accepted: None,
        },
    ] {
        assert_eq!(
            is_loki_json_content_type(&loki_content_type(case.value)).ok(),
            case.accepted
        );
    }
}
