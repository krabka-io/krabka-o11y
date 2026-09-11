use super::{EnvFilter, Layer, LogLevelControl, MakeWriter, Registry, reload};

/// Builds this process's stdout logging layer and the control over its filter.
///
/// The layer is the one `krabka-telemetry` would have installed -- the same
/// `krabka-logfmt` Cloud Logging JSON format over the same `RUST_LOG` filter,
/// falling back to `default_filter` -- wrapped so the filter can be replaced
/// while the process runs.
///
/// [`install_json_logging`](super::install_json_logging) is the call a binary
/// wants. This one exists for a caller that installs the subscriber itself,
/// including a test that captures the lines to check what the filter admitted.
pub fn json_logging_layer<W>(
    default_filter: &str,
    make_writer: W,
) -> (Box<dyn Layer<Registry> + Send + Sync>, LogLevelControl)
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    let level = LogLevelControl::level_word(&filter);
    let (filter, handle) = reload::Layer::new(filter);
    let layer = tracing_subscriber::fmt::layer()
        .event_format(krabka_logfmt::CloudLogging)
        .with_writer(make_writer)
        .with_filter(filter)
        .boxed();
    (layer, LogLevelControl::reloadable(&level, handle))
}
