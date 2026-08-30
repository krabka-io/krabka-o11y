use super::*;

/// Run the symbolizer role with explicit debuginfod resource policy.
///
/// # Errors
///
/// Returns an error when resolver setup fails.
pub async fn run_with_config(
    debuginfod_urls: Vec<String>,
    config: DebuginfodConfig,
) -> Result<(), crate::ProfilesError> {
    let _resolver = native_resolver_from_debuginfod_config(debuginfod_urls.clone(), config)?;
    tracing::info!(
        debuginfod_urls = ?debuginfod_urls,
        "profiles symbolizer ready; DWARF/debuginfod resolver integration is loaded"
    );
    krabka_observability::shutdown_signal().await;
    Ok(())
}
