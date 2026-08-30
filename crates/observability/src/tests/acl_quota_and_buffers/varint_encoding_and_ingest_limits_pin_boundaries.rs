use super::*;

#[test]
pub(crate) fn varint_encoding_and_ingest_limits_pin_boundaries() {
    let mut body = Vec::new();
    encode_varint(0, &mut body);
    encode_varint(127, &mut body);
    encode_varint(128, &mut body);
    encode_varint(300, &mut body);
    assert_eq!(body, vec![0x00, 0x7f, 0x80, 0x01, 0xac, 0x02]);

    let state = DistributorState {
        sink: Arc::new(InMemoryWalSink::default()),
        ingest_limiter: Arc::new(AllowAllIngestLimiter),
        prepare_shutdown: Arc::new(AtomicBool::new(false)),
        metrics: ServiceMetrics::new(),
        max_ingest_body: Some(bytes(5)),
        wal_append_timeout: None,
        reject_old_samples_max_age: None,
        creation_grace_period: None,
    };
    assert!(validate_ingest_body_limit(&state, bytes(5)).is_ok());
    assert!(validate_ingest_body_limit(&state, bytes(6)).is_err());
}
