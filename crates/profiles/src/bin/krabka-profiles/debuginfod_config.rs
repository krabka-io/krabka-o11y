use super::*;

pub(crate) fn debuginfod_config(cli: &Cli) -> Result<DebuginfodConfig, String> {
    let defaults = DebuginfodConfig::default();
    DebuginfodConfig::new(
        cli.debuginfod_max_artifact_size
            .unwrap_or(defaults.max_artifact_size()),
        cli.debuginfod_connect_timeout
            .unwrap_or(defaults.connect_timeout()),
        cli.debuginfod_request_timeout
            .unwrap_or(defaults.request_timeout()),
    )
}
