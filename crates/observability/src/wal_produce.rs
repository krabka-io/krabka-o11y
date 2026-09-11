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

use prometheus_client::{metrics::counter::Counter, registry::Registry};

#[cfg(test)]
mod tests;

mod produce_window;
mod wal_batch_error;
mod wal_produce_metrics;
mod write_batch_pipelined;

pub use self::{
    produce_window::{DEFAULT_PRODUCE_WINDOW, ProduceWindow},
    wal_batch_error::WalBatchError,
    wal_produce_metrics::WalProduceMetrics,
    write_batch_pipelined::write_batch_pipelined,
};
