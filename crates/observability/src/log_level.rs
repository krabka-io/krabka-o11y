//! The process's log filter, and the handle `/log_level` turns.
//!
//! `POST /log_level?log_level=debug` that returns `200 {"status":"success"}`
//! and changes nothing is worse than a 404: an operator who sets `debug`, sees
//! no new lines, and concludes the fault is elsewhere has been sent the wrong
//! way by the thing they were debugging with. So the level this module reports
//! is the one the process is really filtering at, and setting it either moves
//! that filter or says that it cannot.
//!
//! The pinned `krabka-telemetry` installs the subscriber itself and returns no
//! [`reload`](tracing_subscriber::reload) handle, so a process that wants a
//! live filter installs its stdout layer here instead, through
//! [`init_telemetry`]. The layer is the same `krabka-logfmt` Cloud Logging
//! formatter over the same `RUST_LOG` filter; the only difference is the
//! reload handle wrapped around that filter. OTLP export still goes through
//! `krabka-telemetry`, whose exporter internals are private to it -- so a
//! process exporting over OTLP reports a fixed level and says so, rather than
//! accepting a change it cannot make.

use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::MakeWriter,
    layer::{Filter, SubscriberExt},
    reload,
    util::SubscriberInitExt,
};

use crate::{Arc, Error, Mutex, OnceLock};

mod log_level_control;
mod log_level_error;
#[cfg(test)]
mod tests;

pub use log_level_control::LogLevelControl;
pub use log_level_error::LogLevelError;

/// Installs this process's JSON logging and registers the control that
/// `/log_level` drives.
///
/// The first call in a process wins: a later one, or one that finds another
/// subscriber already installed, returns the control already in force rather
/// than a second handle that would move nothing.
pub fn install_json_logging(default_filter: &str) -> LogLevelControl {
    let logging = json_logging_layer(default_filter, std::io::stdout);
    let level = logging.control.level();
    let control = if Registry::default().with(logging.layer).try_init().is_ok() {
        logging.control
    } else {
        // Something already owns this process's subscriber, so the layer just
        // built was dropped with it and its handle moves nothing. Report the
        // level and say that it is fixed.
        LogLevelControl::fixed(&level)
    };
    LogLevelControl::install_process(control)
}

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

pub struct JsonLoggingLayer {
    pub layer: Box<dyn Layer<Registry> + Send + Sync>,
    pub control: LogLevelControl,
}

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
pub fn json_logging_layer<W>(default_filter: &str, make_writer: W) -> JsonLoggingLayer
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    let level = LogLevelControl::level_word(&filter);
    let reloadable = reload::Layer::new(filter);
    let filter = reloadable.0;
    let handle = reloadable.1;
    let layer = tracing_subscriber::fmt::layer()
        .event_format(krabka_logfmt::CloudLogging)
        .with_writer(make_writer)
        .with_filter(filter)
        .boxed();
    JsonLoggingLayer {
        layer,
        control: LogLevelControl::reloadable(&level, handle),
    }
}

/// What a service holds open for as long as it is exporting telemetry.
///
/// Dropping it is not enough: OTLP spans sit in a batch exporter that has to
/// be flushed, so a service calls [`Telemetry::shutdown`] on its way out.
pub struct Telemetry {
    pub(crate) guard: Option<krabka_telemetry::TelemetryGuard>,
}

impl Telemetry {
    /// Flushes and stops whatever exporters this process started.
    pub fn shutdown(self) {
        if let Some(guard) = self.guard {
            guard.shutdown();
        }
    }
}
