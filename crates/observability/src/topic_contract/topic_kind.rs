/// What a topic is for, and therefore what its configuration must hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopicKind {
    /// A write-ahead log. Records are keyed so that one series, trace or
    /// stream stays on one partition, and the block-builder replays them in
    /// offset order.
    ///
    /// `retention.ms` on a WAL topic is not housekeeping. It is the only
    /// backpressure from a stalled block-builder back toward ingest: once the
    /// window passes, the broker drops records the block-builder has not read
    /// yet, and those samples are gone. Size it above the longest
    /// block-builder outage the deployment tolerates.
    Wal,

    /// A log-compacted state topic. The broker keeps the last record per key
    /// and discards the rest, so the topic is a durable map rather than a
    /// stream.
    ///
    /// This only holds with `cleanup.policy=compact`. Under the Kafka default
    /// of `delete`, the broker drops the whole map at the retention window and
    /// the state -- an HA election, a firing alert -- comes back empty.
    CompactedState,
}
