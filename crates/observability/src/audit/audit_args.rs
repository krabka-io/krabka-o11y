use std::{num::NonZeroUsize, path::PathBuf};

use clap::{Args, builder::NonEmptyStringValueParser};
use krabka_ids::PartitionIndex;
use krabka_units::prelude::{ByteSize, Time, gibibytes, secs};

use super::AuditBuildError;

/// The default cap on the audit spool size: 1 GiB, the broker's default.
pub const DEFAULT_AUDIT_SPOOL_MAX: ByteSize = gibibytes(1);

/// The default count of events that the queue to the audit writer holds: the
/// broker's default.
pub const DEFAULT_AUDIT_QUEUE_CAPACITY: NonZeroUsize =
    NonZeroUsize::new(8192).expect("8192 is not zero");

/// The default maximum time between two signed checkpoints: the broker's
/// default.
pub const DEFAULT_AUDIT_CHECKPOINT_EVERY: Time = secs(60);

/// The audit flags that every Krabka service binary flattens into its command
/// line.
///
/// Audit is off while `--audit-topic` is unset. The service then ignores every
/// other audit flag.
#[derive(Args, Clone, Debug, PartialEq)]
pub struct AuditArgs {
    /// Kafka topic that receives the audit records. Default: unset, and audit
    /// is off.
    #[arg(
        id = "audit_topic",
        long = "audit-topic",
        env = "KRABKA_AUDIT_TOPIC",
        value_name = "TOPIC",
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub topic: Option<String>,

    /// Bootstrap servers for the audit producer. Default: the bootstrap
    /// servers of the service's write-ahead log.
    #[arg(
        id = "audit_bootstrap",
        long = "audit-bootstrap",
        env = "KRABKA_AUDIT_BOOTSTRAP",
        value_name = "HOST:PORT",
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub bootstrap: Option<String>,

    /// Partition of the audit topic that this process writes. Default: `0`.
    ///
    /// Each partition holds one hash chain from one writer. Give each process
    /// that writes the topic a different partition.
    #[arg(
        id = "audit_partition",
        long = "audit-partition",
        env = "KRABKA_AUDIT_PARTITION",
        value_name = "PARTITION",
        default_value = "0",
        value_parser = parse_partition
    )]
    pub partition: PartitionIndex,

    /// Directory for the audit spool. Default: unset, and the writer drops
    /// each record that it cannot write to the topic.
    ///
    /// While the topic does not accept writes, the writer keeps records here
    /// and writes them to the topic, in order, when it accepts writes again.
    #[arg(
        id = "audit_spool_dir",
        long = "audit-spool-dir",
        env = "KRABKA_AUDIT_SPOOL_DIR",
        value_name = "PATH"
    )]
    pub spool_dir: Option<PathBuf>,

    /// Cap on the audit spool size. Default: `1GiB`.
    #[arg(
        id = "audit_spool_max",
        long = "audit-spool-max",
        env = "KRABKA_AUDIT_SPOOL_MAX",
        value_name = "SIZE",
        default_value = "1GiB",
        value_parser = krabka_units::parse::positive_byte_size
    )]
    pub spool_max: ByteSize,

    /// Count of events that the queue to the audit writer holds. Default:
    /// `8192`.
    ///
    /// When the queue is full, the service drops the event and counts the
    /// drop. A request never waits for the queue.
    #[arg(
        id = "audit_queue_capacity",
        long = "audit-queue-capacity",
        env = "KRABKA_AUDIT_QUEUE_CAPACITY",
        value_name = "EVENTS",
        default_value = "8192"
    )]
    pub queue_capacity: NonZeroUsize,

    /// Maximum time between two signed checkpoints. Default: `60s`.
    #[arg(
        id = "audit_checkpoint_every",
        long = "audit-checkpoint-every",
        env = "KRABKA_AUDIT_CHECKPOINT_EVERY",
        value_name = "DURATION",
        default_value = "60s",
        value_parser = krabka_units::parse::positive_time
    )]
    pub checkpoint_every: Time,

    /// PKCS#8 Ed25519 key that signs the checkpoints. Default: unset, and the
    /// writer writes no checkpoints.
    #[arg(
        id = "audit_signing_key_path",
        long = "audit-signing-key-path",
        env = "KRABKA_AUDIT_SIGNING_KEY_PATH",
        value_name = "PATH",
        requires = "audit_signing_key_id"
    )]
    pub signing_key_path: Option<PathBuf>,

    /// Key ID that each checkpoint records, and that the verifier looks up.
    /// Set it together with `--audit-signing-key-path`.
    #[arg(
        id = "audit_signing_key_id",
        long = "audit-signing-key-id",
        env = "KRABKA_AUDIT_SIGNING_KEY_ID",
        value_name = "ID",
        requires = "audit_signing_key_path",
        value_parser = NonEmptyStringValueParser::new()
    )]
    pub signing_key_id: Option<String>,
}

impl Default for AuditArgs {
    // The values that `clap` gives when no flag and no variable is set.
    fn default() -> Self {
        Self {
            topic: None,
            bootstrap: None,
            partition: PartitionIndex(0),
            spool_dir: None,
            spool_max: DEFAULT_AUDIT_SPOOL_MAX,
            queue_capacity: DEFAULT_AUDIT_QUEUE_CAPACITY,
            checkpoint_every: DEFAULT_AUDIT_CHECKPOINT_EVERY,
            signing_key_path: None,
            signing_key_id: None,
        }
    }
}

impl AuditArgs {
    /// Whether these flags turn audit on.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.topic.is_some()
    }

    /// The bootstrap servers for the audit producer.
    ///
    /// `--audit-bootstrap` wins. Without it, the audit producer uses
    /// `service_bootstrap`, the bootstrap servers of the service's write-ahead
    /// log.
    ///
    /// # Errors
    ///
    /// Returns [`AuditBuildError::MissingBootstrap`] when neither gives a
    /// bootstrap server.
    pub fn resolve_bootstrap<'a>(
        &'a self,
        service_bootstrap: Option<&'a str>,
    ) -> Result<&'a str, AuditBuildError> {
        self.bootstrap
            .as_deref()
            .or(service_bootstrap)
            .ok_or(AuditBuildError::MissingBootstrap)
    }
}

/// Parses a partition index that is zero or more.
fn parse_partition(input: &str) -> Result<PartitionIndex, String> {
    let index: i32 = input
        .parse()
        .map_err(|error| format!("audit partition `{input}`: {error}"))?;
    if index < 0 {
        return Err(format!("audit partition `{input}` is below zero"));
    }
    Ok(PartitionIndex(index))
}
