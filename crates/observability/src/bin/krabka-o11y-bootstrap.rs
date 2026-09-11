//! Provisions the six Kafka topics the observability stack uses.
//!
//! Run it once per cluster, as a deployment step, before or beside the
//! services. It is safe to run repeatedly and safe to run concurrently with a
//! starting role: it creates what is missing, then describes every topic and
//! refuses to report success when one does not meet the contract.

use clap::Parser;
use krabka_observability::topic_contract::{
    ALL_TOPICS, PartitionCount, TopicSettings, provision_topics,
};
use krabka_units::millis;

#[derive(Debug, Parser)]
#[command(name = "krabka-o11y-bootstrap")]
struct Cli {
    #[arg(long, env = "KRABKA_BOOTSTRAP_SERVER")]
    bootstrap: String,
    /// Partitions on each WAL topic. This is the write-path shard count for
    /// its signal, and raising it later re-maps every key.
    #[arg(long, env = "KRABKA_OBSERVABILITY_WAL_PARTITIONS", default_value_t = 1)]
    partitions: i32,
    /// Partitions on the compacted HA and ruler-state topics.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_STATE_PARTITIONS",
        default_value_t = 1
    )]
    state_partitions: i32,
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_REPLICATION_FACTOR",
        default_value_t = 1
    )]
    replicas: i32,
    /// How far a block-builder may fall behind before the broker starts
    /// dropping WAL records it has not read.
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_WAL_RETENTION_MS",
        default_value_t = 900_000
    )]
    retention_ms: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let settings = TopicSettings {
        wal_partitions: PartitionCount::new(cli.partitions)?,
        state_partitions: PartitionCount::new(cli.state_partitions)?,
        replication_factor: cli.replicas,
        wal_retention: millis(cli.retention_ms),
    };
    let report = provision_topics(&cli.bootstrap, &ALL_TOPICS, &settings).await?;
    for topic in &report.observed {
        println!("{} ready: {} partitions", topic.name, topic.partitions);
    }
    for drift in &report.advisory {
        println!("warning: {drift}");
    }
    Ok(())
}
