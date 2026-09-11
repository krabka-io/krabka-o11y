//! WAL consumer instruments, shared by the four signals.
//!
//! Every signal's ingest path is a Kafka consumer group: a block builder, a
//! hot-tail poller, or a compactor reads the signal's WAL topic and turns it
//! into blocks. A consumer that stops reading loses nothing, because its
//! offsets stay uncommitted, but it also says nothing. The only symptom is a
//! query that returns data which stops at a fixed time, and no instrument in
//! this workspace moved to say so.
//!
//! [`WalConsumerMetrics`] is what moves. One bundle is registered into each
//! signal's registry, so the four signals export the same instrument under
//! their own prefix, and one dashboard reads all four.
//!
//! # What lag this measures, and what it deliberately does not
//!
//! "Records behind" needs the topic's end offset, and that is a broker round
//! trip. It is also a number the broker already has and already exports:
//! `krabka_broker_consumer_group_lag_records{group_id,topic,partition}` is the
//! partition's high watermark minus the group's committed offset, sampled by
//! the broker every 30 seconds from state it holds anyway. Krabka does not
//! compute it a second time. A consumer-side copy would cost one `ListOffsets`
//! request per refresh per consumer for the same number, and the pinned
//! `krabka-client-consumer` gives no cheaper route to it: the `Fetch` response
//! carries the partition's high watermark, but the client reads only
//! `log_start_offset` and `error_code` out of it and exposes neither a
//! `position()` nor an `end_offsets()`. So the consumer-side cost of a record
//! lag here is a whole extra round trip, and the whole extra round trip buys a
//! number that is already scraped.
//!
//! What the broker cannot see is on this side, and all of it is free:
//!
//! - [`receive_delay_seconds`](WalConsumerMetrics::record_poll) is
//!   `now` minus the record's own produce timestamp. It needs no denominator
//!   and no round trip, and it answers the operator's question directly. A
//!   caught-up consumer sits near its poll timeout. A consumer that has fallen
//!   behind climbs without bound. Mimir alerts on the same measurement, as
//!   `cortex_ingest_storage_reader_receive_delay_seconds`. A record whose
//!   producer left the timestamp unset contributes no observation, because the
//!   only alternative is to report the age of the Unix epoch.
//! - `last_consumed_offset` is the offset the reader actually reached. The
//!   broker's lag is against the *committed* offset, so the two differ by
//!   exactly the records a consumer has read and not yet committed, which is
//!   the window a crash would replay.
//! - `polls_total{outcome}` separates the two states that look alike. A
//!   consumer that is caught up keeps counting `empty` polls. A consumer whose
//!   task has died counts nothing at all, and neither does its
//!   `last_consumed_offset` move.
//!
//! # Cardinality
//!
//! The families are labelled by `topic` and `partition` and by nothing else.
//! A partition count is fixed at provisioning by
//! [`topic_contract`](crate::topic_contract), and a role subscribes to one
//! topic, so the series count is the assigned partition count.
//!
//! There is no `tenant` label. This is the hottest loop in the write path, a
//! tenant is read out of the record body rather than out of the poll result,
//! and tenant counts are already recorded once per request at the ingest
//! handler, where the rate is requests and not records.
//!
//! There is no `group_id` label either. One process runs one consumer group
//! per role, the group is a constant for the lifetime of the process, and the
//! broker's own lag series already carries `group_id` for the join.

use krabka_client_consumer::ConsumerRecord;
use krabka_units::{Time, convert::TimeExt};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};

#[cfg(test)]
mod tests;

mod wal_consumer_metrics_bundle;
mod wal_partition_label;
mod wal_poll_outcome;
mod wal_poll_outcome_label;

pub use self::{
    wal_consumer_metrics_bundle::WalConsumerMetrics, wal_partition_label::WalPartitionLabel,
    wal_poll_outcome::WalPollOutcome, wal_poll_outcome_label::WalPollOutcomeLabel,
};
