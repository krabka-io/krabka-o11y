use super::{Time, WalConsumerMetrics};

/// How the hot WAL tail connects to the broker and what it records.
///
/// The fields travel together because they are one decision: which topic this
/// role reads, how hard it reads it, and where the reading is counted.
#[derive(Clone)]
pub struct WalTailConfig {
    /// The broker bootstrap address.
    pub bootstrap: String,
    /// The consumer group this role joins.
    pub group_id: String,
    /// The WAL topic it subscribes to.
    pub wal_topic: String,
    /// How long one poll waits before it returns empty.
    pub poll_timeout: Time,
    /// The Kafka connection's dispatch queue capacity.
    pub client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    /// The Kafka connection's frame maximum.
    pub client_frame_max: krabka_client_core::ClientFrameMax,
    /// The instruments every poll is recorded into.
    pub metrics: WalConsumerMetrics,
    /// TLS and SASL for the consumer. `None` connects in plain text.
    ///
    /// The policy holds the SASL password, and `krabka-client-core` prints it
    /// under `{:?}`. That is why this struct has no `Debug`.
    pub security: Option<krabka_client_core::ClientSecurity>,
}
