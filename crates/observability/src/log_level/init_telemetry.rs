use super::{LogLevelControl, Telemetry, install_json_logging};

pub struct TelemetryStartup {
    pub telemetry: Telemetry,
    pub log_level_control: LogLevelControl,
}

/// Starts a service's telemetry and returns the control `/log_level` drives.
///
/// Without OTLP this installs the stdout subscriber here, so the filter keeps
/// a reload handle and `/log_level` can move it. With OTLP the pinned
/// `krabka-telemetry` installs the subscriber, because the span and log
/// exporters it builds are private to it -- and it returns no reload handle,
/// so the control this returns is [`LogLevelControl::fixed`] and says as much
/// when asked to change.
///
/// # Errors
/// Returns the error `krabka-telemetry` raises when an OTLP exporter cannot be
/// built.
pub fn init_telemetry(
    otlp: Option<krabka_telemetry::OtlpConfig>,
    fmt_default_filter: &str,
    otel_default_filter: &str,
    tracer_name: &str,
) -> Result<TelemetryStartup, krabka_telemetry::TelemetryError> {
    let Some(otlp) = otlp else {
        return Ok(TelemetryStartup {
            telemetry: Telemetry { guard: None },
            log_level_control: install_json_logging(fmt_default_filter),
        });
    };
    let guard = krabka_telemetry::init(
        Some(otlp),
        fmt_default_filter,
        otel_default_filter,
        tracer_name,
    )?;
    let control =
        LogLevelControl::install_process(LogLevelControl::fixed(&LogLevelControl::level_word(
            &tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(fmt_default_filter)),
        )));
    Ok(TelemetryStartup {
        telemetry: Telemetry { guard: Some(guard) },
        log_level_control: control,
    })
}
