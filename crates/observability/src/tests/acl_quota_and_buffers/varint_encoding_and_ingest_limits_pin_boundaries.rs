use super::*;

#[test]
pub(crate) fn varint_encoding_and_ingest_limits_pin_boundaries() {
    let mut body = Vec::new();
    encode_varint(0, &mut body);
    encode_varint(127, &mut body);
    encode_varint(128, &mut body);
    encode_varint(300, &mut body);
    assert_eq!(body, vec![0x00, 0x7f, 0x80, 0x01, 0xac, 0x02]);

    let limits = Limits {
        max_ingest_body: bytes(5),
        ..Limits::unenforced()
    };
    assert!(validate_ingest_body_limit(&limits, bytes(5)).is_ok());
    assert!(validate_ingest_body_limit(&limits, bytes(6)).is_err());
    // Zero is the sentinel for "no cap", so a body of any size passes.
    assert!(validate_ingest_body_limit(&Limits::unenforced(), bytes(6)).is_ok());
}
