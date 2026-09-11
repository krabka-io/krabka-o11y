use std::{
    cell::RefCell,
    num::NonZeroUsize,
    rc::Rc,
    sync::{Arc, Mutex},
};

use assert2::{assert, check};

use super::*;

/// A produce failure with a message, so a test can name which record failed.
#[derive(Debug, thiserror::Error)]
#[error("produce failed: {0}")]
struct FakeProduceError(String);

/// One record for [`FakeProducer::enqueue`].
///
/// `enqueue_yields` delays the enqueue half, which is how a cold metadata
/// fetch behaves. `ack_yields` delays the ack half. `failure` makes the ack
/// fail.
#[derive(Clone, Copy, Debug)]
struct FakeRecord {
    id: usize,
    enqueue_yields: usize,
    ack_yields: usize,
    failure: Option<&'static str>,
}

impl FakeRecord {
    const fn new(id: usize) -> Self {
        Self {
            id,
            enqueue_yields: 0,
            ack_yields: 0,
            failure: None,
        }
    }
}

/// What a fake producer saw, in the order it saw it.
#[derive(Debug, Default)]
struct ProducerLog {
    enqueued: Vec<usize>,
    acked: Vec<usize>,
    in_flight: usize,
    peak_in_flight: usize,
    /// Count of records already enqueued when the first ack resolved.
    enqueued_at_first_ack: Option<usize>,
}

#[derive(Clone, Debug, Default)]
struct FakeProducer {
    log: Arc<Mutex<ProducerLog>>,
}

impl FakeProducer {
    fn log(&self) -> std::sync::MutexGuard<'_, ProducerLog> {
        self.log.lock().expect("fake producer log poisoned")
    }

    fn enqueued(&self) -> Vec<usize> {
        self.log().enqueued.clone()
    }

    fn acked(&self) -> Vec<usize> {
        self.log().acked.clone()
    }

    fn peak_in_flight(&self) -> usize {
        self.log().peak_in_flight
    }

    /// Enqueues `record` and returns its ack.
    async fn enqueue(
        &self,
        record: FakeRecord,
    ) -> Result<impl Future<Output = Result<(), FakeProduceError>> + use<>, FakeProduceError> {
        for _ in 0..record.enqueue_yields {
            tokio::task::yield_now().await;
        }
        {
            let mut log = self.log();
            log.enqueued.push(record.id);
            log.in_flight += 1;
            log.peak_in_flight = log.peak_in_flight.max(log.in_flight);
        }
        let log = Arc::clone(&self.log);
        Ok(async move {
            for _ in 0..record.ack_yields {
                tokio::task::yield_now().await;
            }
            {
                let mut log = log.lock().expect("fake producer log poisoned");
                log.in_flight -= 1;
                let enqueued = log.enqueued.len();
                log.enqueued_at_first_ack.get_or_insert(enqueued);
                if record.failure.is_none() {
                    log.acked.push(record.id);
                }
            }
            match record.failure {
                None => Ok(()),
                Some(message) => Err(FakeProduceError(message.to_owned())),
            }
        })
    }
}

fn window(records: usize) -> ProduceWindow {
    ProduceWindow::new(NonZeroUsize::new(records).expect("test window is not zero"))
}

fn records(count: usize) -> Vec<FakeRecord> {
    (0..count).map(FakeRecord::new).collect()
}

/// The point of the module: the sends of one request overlap. All eight
/// records reach the producer before the first ack resolves, so the batch
/// costs one wait and not eight. A serial loop would show one record in flight
/// at a time.
#[tokio::test]
async fn every_record_is_in_flight_before_the_first_ack_is_awaited() {
    let producer = FakeProducer::default();
    let batch: Vec<_> = records(8)
        .into_iter()
        .map(|record| FakeRecord {
            ack_yields: 1,
            ..record
        })
        .collect();

    let result = write_batch_pipelined(batch, window(8), |record| producer.enqueue(record)).await;

    check!(result.is_ok());
    check!(producer.peak_in_flight() == 8);
    check!(producer.log().enqueued_at_first_ack == Some(8));
    check!(producer.acked().len() == 8);
}

/// The producer fixes per-partition order by the order it accepts records, so
/// the enqueue half must run in argument order. Record 0 here takes the
/// longest to enqueue, which is how a cold metadata fetch behaves. A shape
/// that drives whole send-and-ack futures concurrently would let records 1 and
/// 2 overtake it.
#[tokio::test]
async fn a_slow_enqueue_does_not_let_later_records_overtake_it() {
    let producer = FakeProducer::default();
    let batch = vec![
        FakeRecord {
            enqueue_yields: 4,
            ..FakeRecord::new(0)
        },
        FakeRecord {
            enqueue_yields: 2,
            ..FakeRecord::new(1)
        },
        FakeRecord::new(2),
    ];

    let result = write_batch_pipelined(batch, window(3), |record| producer.enqueue(record)).await;

    check!(result.is_ok());
    check!(producer.enqueued() == vec![0, 1, 2]);
}

/// The window is the whole memory and broker-pressure bound, so it must hold
/// for a batch far larger than itself. Without the bound the count in flight
/// would reach the record count.
#[tokio::test]
async fn the_window_bounds_the_records_in_flight() {
    let producer = FakeProducer::default();
    let batch: Vec<_> = records(100)
        .into_iter()
        .map(|record| FakeRecord {
            ack_yields: 3,
            ..record
        })
        .collect();

    let result = write_batch_pipelined(batch, window(4), |record| producer.enqueue(record)).await;

    check!(result.is_ok());
    check!(producer.peak_in_flight() == 4);
    check!(producer.acked().len() == 100);
}

/// A batch that fails partway is the case this module exists to report. The
/// error must say how many records the broker acked, because the handler turns
/// that into a status code and an instrument, and a partial batch reported as
/// a success is a silent loss.
#[tokio::test]
async fn a_failed_ack_reports_the_count_that_landed() {
    let producer = FakeProducer::default();
    let mut batch = records(10);
    batch[4].failure = Some("broker rejected record 4");

    let result = write_batch_pipelined(batch, window(1), |record| producer.enqueue(record)).await;

    assert!(let Err(error) = &result);
    check!(error.appended() == 4);
    check!(error.total() == 10);
    check!(error.is_partial());
    check!(error.source().to_string() == "produce failed: broker rejected record 4");
    check!(
        error.to_string()
            == "wal append wrote 4 of 10 records: produce failed: broker rejected record 4"
    );
}

/// The first failure stops the enqueue, so the records behind it never reach
/// the producer. The window's worth already in flight is drained first, so the
/// call never returns while this request still has a send outstanding.
#[tokio::test]
async fn a_failure_stops_the_enqueue_and_drains_what_is_in_flight() {
    let producer = FakeProducer::default();
    let mut batch: Vec<_> = records(20)
        .into_iter()
        .map(|record| FakeRecord {
            ack_yields: 2,
            ..record
        })
        .collect();
    batch[1].failure = Some("broker rejected record 1");

    let result = write_batch_pipelined(batch, window(4), |record| producer.enqueue(record)).await;

    assert!(let Err(error) = &result);
    check!(error.total() == 20);
    check!(
        producer.enqueued() == vec![0, 1, 2, 3, 4],
        "the enqueue stopped once the failed ack was seen, well short of 20"
    );
    check!(
        producer.log().in_flight == 0,
        "every send this request made was drained before the call returned"
    );
    check!(
        error.appended() == 4,
        "the four records that were already in flight still acked"
    );
}

/// A batch whose first record fails wrote nothing, and the counts must say
/// that rather than report a partial write.
#[tokio::test]
async fn a_batch_that_fails_on_its_first_record_is_not_partial() {
    let producer = FakeProducer::default();
    let mut batch = records(3);
    batch[0].failure = Some("down");

    let result = write_batch_pipelined(batch, window(1), |record| producer.enqueue(record)).await;

    assert!(let Err(error) = &result);
    check!(error.appended() == 0);
    check!(!error.is_partial());
    check!(producer.acked().is_empty());
}

/// An empty batch is a real shape on this path: a push whose streams were all
/// dropped by relabelling produces no records. It must not be an error.
#[tokio::test]
async fn an_empty_batch_appends_nothing_and_succeeds() {
    let producer = FakeProducer::default();

    let result =
        write_batch_pipelined(Vec::new(), window(4), |record| producer.enqueue(record)).await;

    check!(result.is_ok());
    check!(producer.enqueued().is_empty());
}

/// The default window is the one every sink uses, so its value is part of the
/// contract this module documents.
#[test]
fn the_default_window_is_the_documented_bound() {
    check!(ProduceWindow::default().get() == 1024);
    check!(DEFAULT_PRODUCE_WINDOW.get() == 1024);
}

/// `Rc` is not `Send`, so this only compiles while the combinator stays free
/// of a `Send` bound it does not need. It also pins that the enqueue closure
/// may hold borrowed state.
#[tokio::test]
async fn the_combinator_drives_futures_that_are_not_send() {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let batch = vec![10_usize, 20, 30];

    let result = write_batch_pipelined(batch, window(2), |record| {
        let seen = Rc::clone(&seen);
        async move {
            seen.borrow_mut().push(record);
            Ok::<_, FakeProduceError>(async { Ok(()) })
        }
    })
    .await;

    check!(result.is_ok());
    check!(*seen.borrow() == vec![10, 20, 30]);
}

/// The two produce families answer different questions, so a clean failure and
/// a partial one must not look alike. A batch that appended nothing is a retry
/// that writes each record one time, and it must leave the partial-batch
/// family at zero while still sizing what was lost.
#[test]
fn the_produce_metrics_separate_a_clean_failure_from_a_partial_one() {
    let mut registry = prometheus_client::registry::Registry::default();
    let metrics = WalProduceMetrics::register(&mut registry);

    metrics.record_batch_failure(0, 10);
    check!(metrics.partial_batch_appends() == 0);
    check!(metrics.unappended_records() == 10);

    metrics.record_batch_failure(7, 10);
    check!(metrics.partial_batch_appends() == 1);
    check!(metrics.unappended_records() == 13);
}

/// Both families must reach the registry under the names an alert queries.
#[test]
fn the_produce_metrics_register_under_their_documented_names() {
    let mut registry = prometheus_client::registry::Registry::default();
    let metrics = WalProduceMetrics::register(&mut registry);
    metrics.record_batch_failure(1, 4);

    let mut encoded = String::new();
    prometheus_client::encoding::text::encode(&mut encoded, &registry)
        .expect("encoding a registry into a string");

    check!(encoded.contains("wal_partial_batch_appends_total 1"));
    check!(encoded.contains("wal_unappended_records_total 3"));
}
