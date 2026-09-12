use krabka_blockstore::ProfileIndex;
use krabka_units::convert::TimeExt as _;

use super::{CancellationToken, Cli, ServiceMetrics, build_object_store, debuginfod_config};

/// Runs the symbolizer as a process of its own.
///
/// Each pass scans the current profile index, resolves native addresses through
/// the configured filesystem/debuginfod chain, and writes updated symbol
/// databases beside their blocks. `--target all` runs the same loop through
/// [`symbolizer_stage`](super::symbolizer_stage).
///
/// # Errors
/// Returns an error when configuration, object-store access, index loading, or
/// symbolization fails.
pub(crate) async fn run_symbolizer(
    cli: Cli,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = debuginfod_config(&cli)?;
    let resolver = krabka_profiles::symbolizer::offline_resolver_from_debuginfod_config(
        cli.debuginfod_urls,
        config,
    )?;
    let configured = build_object_store(&cli.object_store_url, metrics.object_store)
        .map_err(|error| format!("object store: {error}"))?;
    loop {
        let index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
            &configured.store,
            &cli.index_object_key,
            cli.index_snapshot_max,
        )
        .await?;
        let updated = krabka_profiles::symbolizer::symbolize_blocks_once(
            &configured.store,
            &index,
            &resolver,
        )
        .await?;
        tracing::info!(updated, "profiles offline symbolization pass complete");
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(cli.index_refresh_interval.to_std()) => {}
        }
    }
}
