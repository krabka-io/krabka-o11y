use std::sync::Arc;

use krabka_blockstore::TenantId;
use krabka_metrics::{SamplePayload, WalRecord};
use krabka_promql::{EngineOpts, MetricStore, PromqlEngine, QueryResult, WalHead};

use crate::{
    Offset, PartitionIndex, WalHeadConsumerRecord, WalHeadPartitionOffset, WalHeadReplayResult,
    replay_wal_head_records,
};

#[tokio::test]
async fn production_replay_preserves_queries_offsets_and_decode_rollback() {
    let head = WalHead::new();
    let before = head.snapshot();
    let record = |tenant: &str, timestamp_ms, value, start_timestamp_ms| WalRecord {
        tenant: tenant.into(),
        labels: [
            ("job", "old"),
            ("__name__", "up"),
            ("é", "🦀"),
            ("job", "api"),
        ]
        .map(|(k, v)| (k.into(), v.into()))
        .into(),
        payload: SamplePayload::Float {
            timestamp_ms,
            value,
            start_timestamp_ms,
        },
        exemplars: Vec::new(),
    };
    let encoded = record("tenant-a", 9_000, 3.0, Some(1_000))
        .encode()
        .unwrap();
    let wire = |partition, offset, value| WalHeadConsumerRecord {
        topic: krabka_metrics::WAL_TOPIC.into(),
        partition: PartitionIndex(partition),
        offset: Offset(offset),
        value,
    };
    // A valid prefix followed by invalid bytes must publish nothing.
    assert2::assert!(
        replay_wal_head_records(
            &head,
            krabka_metrics::WAL_TOPIC,
            &[
                wire(0, 7, Some(encoded.clone())),
                wire(0, 8, Some(vec![0xff])),
            ]
        )
        .is_err()
    );
    assert2::assert!(
        head.series("tenant-a", &[], i64::MIN, i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    assert2::assert!(head.high_water_offset(PartitionIndex(0)).is_none());
    let records = [
        WalHeadConsumerRecord {
            topic: "other-topic".into(),
            partition: PartitionIndex(9),
            offset: Offset(90),
            value: None,
        },
        wire(0, 7, Some(encoded)),
        wire(
            0,
            8,
            Some(
                record("tenant-a", 11_000, 7.0, Some(5_000))
                    .encode()
                    .unwrap(),
            ),
        ),
        wire(
            0,
            9,
            Some(record("tenant-a", 11_000, 99.0, None).encode().unwrap()),
        ),
        wire(
            1,
            20,
            Some(record("tenant-b", 10_000, 21.0, None).encode().unwrap()),
        ),
    ];
    let bytes = records.iter().map(|r| r.value.clone()).collect::<Vec<_>>();
    let actual = replay_wal_head_records(&head, krabka_metrics::WAL_TOPIC, &records).unwrap();
    assert2::assert!(
        actual
            == WalHeadReplayResult {
                polled_records: 5,
                replayed_records: 4,
                committed_offsets: vec![
                    WalHeadPartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(10)
                    },
                    WalHeadPartitionOffset {
                        partition: PartitionIndex(1),
                        offset: Offset(21)
                    }
                ],
            }
    );
    assert2::assert!(records.iter().map(|r| r.value.clone()).collect::<Vec<_>>() == bytes);
    assert2::assert!(head.low_water_offset(PartitionIndex(0)) == Some(Offset(7)));
    assert2::assert!(head.high_water_offset(PartitionIndex(0)) == Some(Offset(9)));
    assert2::assert!(head.low_water_offset(PartitionIndex(1)) == Some(Offset(20)));
    assert2::assert!(head.high_water_offset(PartitionIndex(1)) == Some(Offset(20)));
    assert2::assert!(head.high_water_offset(PartitionIndex(9)).is_none());
    assert2::assert!(
        before
            .series("tenant-a", &[], i64::MIN, i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    let engine = PromqlEngine::new(Arc::new(head), EngineOpts::default());
    for (tenant, value) in [("tenant-a", 99.0), ("tenant-b", 21.0)] {
        let expected: QueryResult = serde_json::from_value(serde_json::json!({
            "InstantVector": [{"labels":{"__name__":"up","job":"api","é":"🦀"},"ts_ms":12_000,"value":{"Float":value}}]
        })).unwrap();
        assert2::assert!(
            engine
                .query_instant(&TenantId::new(tenant).unwrap(), "up", 12_000)
                .await
                .unwrap()
                == expected
        );
    }
}
