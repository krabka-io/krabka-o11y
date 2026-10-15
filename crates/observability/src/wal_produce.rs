//! Bounded, ordered produce for the WAL ingest paths of the four signals.
//!
//! Every signal turns one ingest request into many WAL records: a remote-write
//! body becomes one record per sample, a Loki push becomes one record per
//! entry, an OTLP export becomes one record per span. The naive loop awaits
//! each record's broker ack before it sends the next, so a body that carries
//! 10,000 series costs 10,000 serial round trips.
//!
//! [`write_batch_pipelined`] removes the serial wait and keeps two properties
//! that the loop got for free.
//!
//! # Order
//!
//! Kafka orders records per partition by the order the producer enqueues
//! them, and the WAL topics key on the series, the stream or the trace, so one
//! entity's records all land on one partition. The pinned
//! `krabka-client-producer` splits the two halves of a send:
//! `Producer::send` appends the record to the partition accumulator and
//! returns a `oneshot::Receiver` for the ack. Only the first half decides the
//! order.
//!
//! So this module enqueues serially and waits concurrently. The enqueue order
//! is the argument order, and per-key order survives. A shape that drives
//! whole `send`-and-ack futures with `join_all` does not give this: the
//! enqueue halves then interleave at whatever await point each future reaches
//! first, and a metadata fetch on a cold topic is such a point.
//!
//! # Bound
//!
//! `window` caps the records that are enqueued and not yet acked. The cap
//! matters because the accumulator queues every record the sender has not
//! drained, so an unbounded fire of a 10,000-record body holds a second
//! encoded copy of the whole body in the producer, and offers the broker a
//! burst that no configured limit shapes.
//!
//! # Partial failure
//!
//! A serial loop had one virtue: the failure index was the written count. A
//! pipelined batch loses that, so [`WalBatchError`] carries it instead. The
//! first failed ack stops the enqueue, and the records already in flight are
//! drained and counted, so the error states how many of how many records
//! reached the broker.
//!
//! A caller must not report a partial batch as a success. Each signal's
//! upstream client retries the whole request on a retriable status, which is
//! why the counts belong in the error and in [`WalProduceMetrics`], not in a
//! 2xx.

use std::{collections::VecDeque, future::Future, num::NonZeroUsize};

use axum::body::Bytes;
use krabka_client_producer::Header as ProducerHeader;
use crate::persisted_format::{PERSISTED_FORMAT_HEADER, PERSISTED_FORMAT_VERSION};
use prometheus_client::{metrics::counter::Counter, registry::Registry};

#[cfg(test)]
mod tests;

mod wal_produce_metrics;

pub use wal_produce_metrics::WalProduceMetrics;

/// Records that one request may hold enqueued and unacked.
///
/// The pinned producer defaults to a 16 KiB batch and five in-flight requests
/// per partition, so about 400 records of a typical 200-byte WAL record fill
/// one partition's in-flight budget. A window of 1024 keeps several
/// partitions' budgets full and still caps what a single request queues inside
/// the producer.
pub const DEFAULT_PRODUCE_WINDOW: NonZeroUsize = NonZeroUsize::new(1024).expect("1024 is not zero");

/// The count of records that may be in flight at one time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProduceWindow(NonZeroUsize);

impl ProduceWindow {
    /// Builds a window of `records`.
    #[must_use]
    pub const fn new(records: NonZeroUsize) -> Self {
        Self(records)
    }

    /// Returns the window as a count.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl Default for ProduceWindow {
    fn default() -> Self {
        Self(DEFAULT_PRODUCE_WINDOW)
    }
}

/// A WAL batch that did not append in full, and how far it got.
///
/// `appended` is the count of records the broker acked. `total` is the count
/// the request asked for. `appended` is always below `total`, and a value
/// above zero means the batch landed in part. The handler that maps this to a
/// status code must not report a partial batch as a success.
#[derive(Debug, thiserror::Error)]
#[error("wal append wrote {appended} of {total} records: {source}")]
pub struct WalBatchError<E>
where
    E: std::error::Error + 'static,
{
    appended: usize,
    total: usize,
    #[source]
    source: E,
}

impl<E> WalBatchError<E>
where
    E: std::error::Error + 'static,
{
    /// Builds the error from the counts and the failure that stopped the batch.
    #[must_use]
    pub const fn new(appended: usize, total: usize, source: E) -> Self {
        Self {
            appended,
            total,
            source,
        }
    }

    /// Returns the count of records the broker acked.
    #[must_use]
    pub const fn appended(&self) -> usize {
        self.appended
    }

    /// Returns the count of records the request asked to append.
    #[must_use]
    pub const fn total(&self) -> usize {
        self.total
    }

    /// Reports whether part of the batch is durable.
    ///
    /// A partial batch is the case a retry duplicates, because every upstream
    /// client here retries the whole request.
    #[must_use]
    pub const fn is_partial(&self) -> bool {
        self.appended > 0
    }

    /// Returns the failure that stopped the batch.
    #[must_use]
    pub const fn source(&self) -> &E {
        &self.source
    }

    /// Consumes the error and returns the failure that stopped the batch.
    #[must_use]
    pub fn into_source(self) -> E {
        self.source
    }
}

/// Returns the Kafka headers every signal stamps on a WAL record.
///
/// The persisted-format version header comes first. The current span's W3C
/// trace context (`traceparent`/`tracestate`) follows, so the WAL consumer can
/// re-parent its span onto the ingest trace. The trace headers are additive:
/// there are none when no span is active and sampled.
#[must_use]
pub fn wal_record_headers() -> Vec<ProducerHeader> {
    std::iter::once(ProducerHeader {
        key: PERSISTED_FORMAT_HEADER.to_string(),
        value: Some(Bytes::from_static(PERSISTED_FORMAT_VERSION)),
    })
    .chain(
        krabka_telemetry::propagation::current_trace_headers()
            .into_iter()
            .map(|header| ProducerHeader {
                key: header.0,
                value: Some(Bytes::from(header.1.into_bytes())),
            }),
    )
    .collect()
}

/// Appends `records` with a bounded window of unacked sends.
///
/// `enqueue` hands one record to the producer and returns the future that
/// resolves when the broker acks it. The two halves must be separate: this
/// function awaits every `enqueue` in argument order, so the producer sees the
/// records in that order, and it awaits the acks out of that order.
///
/// The first failure stops the enqueue. The sends already in flight are
/// drained, so the call does not return while the producer still holds records
/// from this request, and the acks they resolve to are counted.
///
/// # Errors
/// Returns [`WalBatchError`] when a send or an ack fails. The error carries the
/// count of records the broker acked, which is below the count asked for.
pub async fn write_batch_pipelined<Record, Ack, Error, Enqueue, Enqueued>(
    records: Vec<Record>,
    window: ProduceWindow,
    enqueue: Enqueue,
) -> Result<(), WalBatchError<Error>>
where
    Enqueue: Fn(Record) -> Enqueued,
    Enqueued: Future<Output = Result<Ack, Error>>,
    Ack: Future<Output = Result<(), Error>>,
    Error: std::error::Error + 'static,
{
    let total = records.len();
    let mut records = records.into_iter();
    let mut in_flight: VecDeque<Ack> = VecDeque::with_capacity(window.get());
    let mut appended = 0_usize;
    let mut failure: Option<Error> = None;

    loop {
        while failure.is_none() && in_flight.len() < window.get() {
            let Some(record) = records.next() else { break };
            match enqueue(record).await {
                Ok(ack) => in_flight.push_back(ack),
                Err(error) => failure = Some(error),
            }
        }
        let Some(ack) = in_flight.pop_front() else {
            break;
        };
        match ack.await {
            Ok(()) => appended += 1,
            Err(error) => {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
    }

    failure.map_or(Ok(()), |source| {
        Err(WalBatchError::new(appended, total, source))
    })
}

/// Appends `records` one at a time, each waiting for its ack.
///
/// This is the batch append for a sink with no produce pipeline: it awaits
/// `append` for each record in order and stops at the first failure.
/// [`write_batch_pipelined`](write_batch_pipelined) is the windowed
/// form for a sink that can keep several sends in flight.
///
/// # Errors
/// Returns [`WalBatchError`] when a record fails to append. The error carries
/// the count of records that did append before it.
pub async fn write_batch_serially<Record, Error, Append, Appended>(
    records: Vec<Record>,
    append: Append,
) -> Result<(), WalBatchError<Error>>
where
    Append: Fn(Record) -> Appended,
    Appended: Future<Output = Result<(), Error>>,
    Error: std::error::Error + 'static,
{
    let total = records.len();
    for item in records.into_iter().enumerate() {
        let appended = item.0;
        let record = item.1;
        if let Err(source) = append(record).await {
            return Err(WalBatchError::new(appended, total, source));
        }
    }
    Ok(())
}
