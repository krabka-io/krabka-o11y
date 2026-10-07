mod alloc;

use clap::Parser;
use krabka_metrics::runtime::{RuntimeConfig, run};
use krabka_observability::{argv_with_config_file, server_security::install_crypto_provider};
use krabka_telemetry::OtlpConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // First: rustls has two crypto providers compiled in, and the first TLS
    // client or server that asks for the process default panics without one.
    install_crypto_provider();
    let cli =
        RuntimeConfig::parse_from(argv_with_config_file::<RuntimeConfig>(std::env::args_os())?);
    let telemetry = krabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "krabka-metrics",
            env!("CARGO_PKG_VERSION"),
            "krabka-metrics",
        )?,
        "krabka_metrics=info,info",
        "info",
        "krabka-metrics",
    )?;
    let result = run(cli).await;
    telemetry.shutdown();
    result
}
