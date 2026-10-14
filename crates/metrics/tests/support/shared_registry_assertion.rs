macro_rules! assert_shared_registry_metrics {
    ($metrics:expr, $prefix:literal, [$($needle:expr),* $(,)?]) => {{
        let m = $metrics;
        m.wal_consumer.record_partition_assigned("__wal", 2);
        m.wal_produce.record_batch_failure(1, 3);
        m.compaction.record_output(4);
        m.object_store.record_retry(ObjectStoreOperation::Get);

        let buf = encode_registry(&m.registry).await.unwrap();
        for needle in [
            concat!($prefix, "_wal_consumer_partition_owned{topic=\"__wal\",partition=\"2\"} 1"),
            concat!($prefix, "_wal_partial_batch_appends_total 1"),
            concat!($prefix, "_compaction_blocks_total 4"),
            concat!($prefix, "_objstore_operation_retries_total{operation=\"get\"} 1"),
            $($needle),*
        ] {
            assert2::assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
        buf
    }};
}
