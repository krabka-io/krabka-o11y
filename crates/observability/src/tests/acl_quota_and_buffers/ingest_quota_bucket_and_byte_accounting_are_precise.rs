use super::*;

#[test]
pub(crate) fn ingest_quota_bucket_and_byte_accounting_are_precise() {
    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]),
        timestamp_ns: 42,
        line: "hello".to_string(),
        structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
        position: None,
    };
    let expected_bytes = "tenant-a".len()
        + "hello".len()
        + std::mem::size_of::<i64>()
        + "app".len()
        + "api".len()
        + "env".len()
        + "prod".len()
        + "trace_id".len()
        + "abc".len();
    check!(ingest_quota_bytes(&[record]) == measured_size(expected_bytes));

    let mut bucket = IngestQuotaBucket::new(bytes_per_sec(10), secs(1));
    check!(bucket.capacity() == bytes(10));
    check!(bucket.consume(bytes(10)));
    check!(!bucket.consume(ByteSize::from_bytes_f64(0.1)));
    bucket.update_rate(bytes_per_sec(5));
    check!(bucket.available <= bytes(5));
    bucket.available = bytes(4);
    bucket.update_rate(bytes_per_sec(20));
    check!(bucket.available >= bytes(4));
    check!(bucket.consume(bytes(4)));

    // Neither assertion above reaches the clamp: the bucket is empty by
    // then, so nothing is banked over the new capacity and `available`
    // could have been left alone -- or topped up to the new capacity --
    // without either inequality noticing.
    //
    // Lowering the rate shrinks the capacity, and what was banked above it
    // is given up.
    bucket.available = bytes(20);
    bucket.update_rate(bytes_per_sec(5));
    check!(bucket.available == bytes(5), "clamped to the new capacity");

    // Raising it grows the capacity and hands out nothing: a bucket
    // refills over time, not on a configuration change. The bound is loose
    // by a byte because `update_rate` refills first, over however long the
    // two statements took.
    bucket.available = bytes(2);
    bucket.update_rate(bytes_per_sec(50));
    check!(
        bucket.available < bytes(3),
        "not topped up to the new capacity"
    );

    // Refilling adds the rate over however long has passed. Every case
    // above reaches it only through `update_rate`, which calls it against
    // a bucket whose clock has not moved -- so with the body removed they
    // all behave the same.
    let mut refilling = IngestQuotaBucket::new(bytes_per_sec(10), secs(1));
    refilling.available = bytes(0);
    refilling.updated_at = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_millis(500))
        .expect("the process has been running for at least half a second");
    refilling.refill();
    check!(
        refilling.available >= bytes(5),
        "half a second at ten bytes a second is at least five bytes"
    );
    check!(
        refilling.available <= bytes(10),
        "and never past the capacity"
    );

    let bucket = IngestQuotaBucket::new(bytes_per_sec(10), secs(2));
    check!(bucket.capacity() == bytes(20));
}
