use super::*;

/// Accumulating a WAL batch stops on two conditions, and neither had ever
/// been reached. An empty first poll returns straight away rather than
/// waiting out the accumulation window for records that are not coming;
/// and once the batch is full the loop stops, rather than taking one more
/// poll's worth beyond the cap it was given.
#[tokio::test]
pub(crate) async fn accumulating_a_wal_batch_stops_when_empty_or_full() {
    struct ScriptedConsumer {
        pub(crate) batches: std::collections::VecDeque<Vec<WalRecordForTest>>,
    }
    type WalRecordForTest = super::super::prelude::KafkaWalRecord;

    #[async_trait]
    impl super::super::prelude::LogWalConsumer for ScriptedConsumer {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<Vec<super::super::prelude::KafkaWalRecord>, super::super::prelude::WalConsumerError> {
            Ok(self.batches.pop_front().unwrap_or_default())
        }
        async fn commit_compacted(
            &mut self,
            _position: super::super::prelude::WalPosition,
        ) -> Result<(), super::super::prelude::WalConsumerError> {
            Ok(())
        }
    }

    let record = |offset: i64| super::super::prelude::KafkaWalRecord {
        value: Vec::new(),
        partition: PartitionIndex(0),
        offset: Offset(offset),
        timestamp_ms: None,
        headers: Vec::new(),
    };
    let poll = |batches: Vec<Vec<super::super::prelude::KafkaWalRecord>>, max: usize| async move {
        let mut consumer = ScriptedConsumer {
            batches: batches.into_iter().collect(),
        };
        super::super::prelude::poll_accumulated_log_compaction_records(
            &mut consumer,
            secs(1),
            secs(5),
            millis(10),
            NonZeroUsize::new(max).expect("a positive cap"),
        )
        .await
        .expect("the scripted consumer does not fail")
    };

    // An empty first poll is the answer, not the start of a wait: the
    // batch waiting behind it must not be drawn in.
    let empty = poll(vec![vec![], vec![record(1)]], 3).await;
    check!(empty.is_empty(), "an empty poll returns empty");

    // One short of the cap accumulates; reaching the cap stops, leaving
    // the batch behind it alone.
    let full = poll(
        vec![vec![record(1)], vec![record(2), record(3)], vec![record(4)]],
        3,
    )
    .await;
    check!(full.len() == 3, "stops at the cap, got {}", full.len());
}
