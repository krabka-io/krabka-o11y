use krabka_units::convert::TimeExt;

use crate::{
    Acks, Arc, AutoOffsetReset, BTreeMap, Bytes, CancellationToken, ClientResourcePolicy,
    CompactionFrontier, Consumer, ConsumerError, HotTailPollError, JoinHandle, KafkaWalHeader,
    KafkaWalRecord, LogHotTail, LogWalConsumer, LogWalSink, Mutex, Offset, PartitionIndex,
    Producer, ProducerError, ProducerHeader, ProducerRecord, SharedCompactionFrontier, Time,
    WalConsumerError, WalLogRecord, WalPosition, WalRecordDecodeError, WalSinkError, async_trait,
    decode_native_kafka_log_record, has_native_kafka_log_headers, hot_tail_bucket_key, minutes,
    series_fingerprint, sleep,
};

// === split-modules: generated submodules ===
mod buffered_log_hot_tail;
mod build_kafka_wal_record;
mod decode_kafka_wal_record;
mod decode_kafka_wal_record_envelope;
mod hot_tail_buffer;
mod kafka_log_wal_consumer;
mod kafka_log_wal_sink;
mod poll_log_hot_tail_once;
mod poll_log_hot_tail_once_with_frontier;
mod spawn_log_hot_tail_poller;

pub use buffered_log_hot_tail::BufferedLogHotTail;
pub use build_kafka_wal_record::build_kafka_wal_record;
pub use decode_kafka_wal_record::decode_kafka_wal_record;
pub use decode_kafka_wal_record_envelope::decode_kafka_wal_record_envelope;
pub (crate) use hot_tail_buffer::HotTailBuffer;
pub use kafka_log_wal_consumer::KafkaLogWalConsumer;
pub use kafka_log_wal_sink::KafkaLogWalSink;
pub use poll_log_hot_tail_once::poll_log_hot_tail_once;
pub (crate) use poll_log_hot_tail_once_with_frontier::poll_log_hot_tail_once_with_frontier;
# [cfg_attr (test , mutants :: skip)] pub (crate) use spawn_log_hot_tail_poller::spawn_log_hot_tail_poller;
