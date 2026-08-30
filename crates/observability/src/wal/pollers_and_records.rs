use krabka_units::convert::TimeExt;

use crate::{
    Arc, AtomicOrdering, BTreeMap, BrokerBackedQueryAuthorizer, BufferedLogHotTail,
    CancellationToken, ClientResourcePolicy, ConsumerError, DeferredWalConsumerConnect, Error,
    JoinHandle, KafkaLogWalConsumer, KafkaWalHeader, KafkaWalRecord, LogQueryAuthorizer,
    ProducerError, ServiceReadiness, SharedCompactionFrontier, Time, WalLogRecord, WalPosition,
    is_loki_label_name, poll_log_hot_tail_once_with_frontier, sleep,
};

mod decode_native_kafka_log_record;
mod has_native_kafka_log_headers;
mod hot_tail_poll_error;
mod ingest_limit_error;
mod kafka_headers_with_prefix;
mod native_timestamp_ms_to_ns;
mod optional_kafka_header_utf8;
mod query_authorization_error;
mod required_kafka_header_utf8;
mod spawn_query_authorizer_connect;
mod spawn_wal_hot_tail_connect_and_poll;
mod validate_native_timestamp_ns;
mod wal_consumer_error;
mod wal_record_decode_error;
mod wal_sink_error;

pub(crate) use decode_native_kafka_log_record::decode_native_kafka_log_record;
pub(crate) use has_native_kafka_log_headers::has_native_kafka_log_headers;
pub use hot_tail_poll_error::HotTailPollError;
pub use ingest_limit_error::IngestLimitError;
pub(crate) use kafka_headers_with_prefix::kafka_headers_with_prefix;
pub(crate) use native_timestamp_ms_to_ns::native_timestamp_ms_to_ns;
pub(crate) use optional_kafka_header_utf8::optional_kafka_header_utf8;
pub use query_authorization_error::QueryAuthorizationError;
pub(crate) use required_kafka_header_utf8::required_kafka_header_utf8;
#[cfg_attr(test, mutants::skip)]
pub(crate) use spawn_query_authorizer_connect::spawn_query_authorizer_connect;
#[cfg_attr(test, mutants::skip)]
pub(crate) use spawn_wal_hot_tail_connect_and_poll::spawn_wal_hot_tail_connect_and_poll;
pub(crate) use validate_native_timestamp_ns::validate_native_timestamp_ns;
pub use wal_consumer_error::WalConsumerError;
pub use wal_record_decode_error::WalRecordDecodeError;
pub use wal_sink_error::WalSinkError;
