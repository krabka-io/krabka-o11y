use super::{
    LogLevelControl, Registry, SubscriberExt as _, SubscriberInitExt as _, json_logging_layer,
};

/// Installs this process's JSON logging and registers the control that
/// `/log_level` drives.
///
/// The first call in a process wins: a later one, or one that finds another
/// subscriber already installed, returns the control already in force rather
/// than a second handle that would move nothing.
pub fn install_json_logging(default_filter: &str) -> LogLevelControl {
    let (layer, control) = json_logging_layer(default_filter, std::io::stdout);
    let level = control.level();
    let control = if Registry::default().with(layer).try_init().is_ok() {
        control
    } else {
        // Something already owns this process's subscriber, so the layer just
        // built was dropped with it and its handle moves nothing. Report the
        // level and say that it is fixed.
        LogLevelControl::fixed(&level)
    };
    LogLevelControl::install_process(control)
}
