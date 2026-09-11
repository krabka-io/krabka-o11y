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

mod init_telemetry;
mod install_json_logging;
mod json_logging_layer;
mod log_level_control;
mod log_level_error;
mod telemetry;

pub use init_telemetry::init_telemetry;
pub use install_json_logging::install_json_logging;
pub use json_logging_layer::json_logging_layer;
pub use log_level_control::LogLevelControl;
pub use log_level_error::LogLevelError;
pub use telemetry::Telemetry;
