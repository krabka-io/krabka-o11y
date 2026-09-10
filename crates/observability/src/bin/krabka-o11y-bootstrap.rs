use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "krabka-o11y-bootstrap")]
struct Cli {
    #[arg(long, env = "KRABKA_BOOTSTRAP_SERVER")]
    bootstrap: String,
    #[arg(long, env = "KRABKA_OBSERVABILITY_WAL_PARTITIONS", default_value_t = 1)]
    partitions: i32,
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_REPLICATION_FACTOR",
        default_value_t = 1
    )]
    replicas: i32,
    #[arg(
        long,
        env = "KRABKA_OBSERVABILITY_WAL_RETENTION_MS",
        default_value_t = 900_000
    )]
    retention_ms: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    krabka_observability::topic_contract::bootstrap_topics(
        &cli.bootstrap,
        cli.partitions,
        cli.replicas,
        cli.retention_ms,
    )
    .await?;
    println!("observability topics ready");
    Ok(())
}
