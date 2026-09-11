use super::{Arc, EnvFilter, Filter, LogLevelError, Mutex, OnceLock, Registry, reload};

/// This process's log level, and the handle that moves it.
///
/// A clone names the same filter as the original, so the copy an HTTP route
/// holds and the copy start-up kept are the same control.
#[derive(Clone)]
pub struct LogLevelControl {
    state: Arc<LogLevelState>,
}

struct LogLevelState {
    level: Mutex<String>,
    reload: Option<reload::Handle<EnvFilter, Registry>>,
}

/// The control this process installed, if it installed one.
static PROCESS_LOG_LEVEL: OnceLock<LogLevelControl> = OnceLock::new();

impl LogLevelControl {
    pub(crate) fn reloadable(level: &str, handle: reload::Handle<EnvFilter, Registry>) -> Self {
        Self {
            state: Arc::new(LogLevelState {
                level: Mutex::new(level.to_owned()),
                reload: Some(handle),
            }),
        }
    }

    /// A control over a filter that cannot be moved after start-up.
    ///
    /// It still reports the level truthfully. [`LogLevelControl::set_level`]
    /// on it fails with [`LogLevelError::Fixed`] rather than reporting a
    /// change that did not happen.
    #[must_use]
    pub fn fixed(level: &str) -> Self {
        Self {
            state: Arc::new(LogLevelState {
                level: Mutex::new(level.to_owned()),
                reload: None,
            }),
        }
    }

    /// Records `control` as the one this process's `/log_level` route drives.
    ///
    /// The first call wins and later calls leave it alone, so a test binary
    /// that installs logging from more than one test still has one control.
    /// Returns the control now in force, which is `control` only for the call
    /// that won.
    pub fn install_process(control: Self) -> Self {
        PROCESS_LOG_LEVEL.get_or_init(|| control).clone()
    }

    /// The control this process installed.
    ///
    /// A process that installed none -- a test binary with no subscriber, or
    /// an embedder that built its own -- gets a fixed control reporting the
    /// level `RUST_LOG` asks for, which is what such a process is really
    /// filtering at.
    #[must_use]
    pub fn process() -> Self {
        PROCESS_LOG_LEVEL.get().cloned().unwrap_or_else(|| {
            Self::fixed(&Self::level_word(
                &EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            ))
        })
    }

    /// The level the process is filtering at now.
    ///
    /// # Panics
    /// Panics if another thread panicked while holding the level.
    #[must_use]
    pub fn level(&self) -> String {
        self.state.level.lock().expect("log level").clone()
    }

    /// Whether [`LogLevelControl::set_level`] can move this process's filter.
    #[must_use]
    pub fn is_reloadable(&self) -> bool {
        self.state.reload.is_some()
    }

    /// Replaces the process's filter with `level`.
    ///
    /// The new filter is `level` alone, so the per-target directives start-up
    /// read from `RUST_LOG` stop applying until the level is set back. That is
    /// what an operator asking for `debug` is asking for.
    ///
    /// # Errors
    /// Returns [`LogLevelError::Fixed`] when this process installed its
    /// logging without a reload handle, and [`LogLevelError::Reload`] when the
    /// subscriber holding the filter is gone.
    ///
    /// # Panics
    /// Panics if another thread panicked while holding the level.
    pub fn set_level(&self, level: &str) -> Result<(), LogLevelError> {
        let Some(handle) = self.state.reload.as_ref() else {
            return Err(LogLevelError::Fixed);
        };
        handle
            .reload(EnvFilter::new(level))
            .map_err(|error| LogLevelError::Reload(error.to_string()))?;
        level.clone_into(&mut self.state.level.lock().expect("log level"));
        Ok(())
    }

    /// The single word that names what a filter admits, as `/log_level`
    /// reports it.
    pub(crate) fn level_word(filter: &EnvFilter) -> String {
        <EnvFilter as Filter<Registry>>::max_level_hint(filter).map_or_else(
            || "info".to_owned(),
            |level| level.to_string().to_lowercase(),
        )
    }
}
