use super::{Cli, debuginfod_config};

/// Runs the symbolizer as a process of its own.
///
/// Pyroscope has a `symbolizer` target and so does this binary, but in both
/// the symbolization itself happens inside the read path rather than in this
/// process: a profiles querier resolves native addresses through the same
/// resolver chain, built from the same `--debuginfod-url` list. Standing the
/// role up on its own therefore proves the configuration and holds the
/// resolver open, and nothing queries it. `--target all` runs it as its own
/// stage for the same reason, but without the `SIGTERM` wait below -- see
/// [`symbolizer_stage`](super::symbolizer_stage).
///
/// # Errors
/// Returns an error when the debuginfod resource policy is out of range, or
/// when a configured debuginfod URL cannot be turned into an HTTP client.
pub(crate) async fn run_symbolizer(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let config = debuginfod_config(&cli)?;
    krabka_profiles::symbolizer::run_with_config(cli.debuginfod_urls, config).await?;
    Ok(())
}
