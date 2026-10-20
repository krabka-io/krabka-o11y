//! The child half of a `SIGTERM` suite: a runtime that already holds the
//! signal, and the free ports the parent names before the child starts.
//!
//! The `SIGTERM` suites of this binary and of `//crates/metrics-service` reach
//! this file with `#[path]`, so it depends only on `tokio`.

/// A multi-thread runtime with a `SIGTERM` handler registered on it.
///
/// The handler is registered before the parent can reach anything the child
/// binds or exports, so the parent's `kill` cannot land in the window before
/// the role installs its own. Tokio's handlers are process-wide and
/// refcounted, so the one the role installs later is this same registration.
pub struct SigtermChildRuntime {
    // Declared first so it drops before the runtime, as a local declared
    // after the runtime would.
    _terminate: tokio::signal::unix::Signal,
    runtime: tokio::runtime::Runtime,
}

impl SigtermChildRuntime {
    /// Builds the runtime and registers the handler on it.
    ///
    /// # Panics
    ///
    /// Panics when the runtime cannot be built or the handler installed.
    pub fn start() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("child runtime");
        let terminate = runtime
            .block_on(async {
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            })
            .expect("install SIGTERM handler");
        Self {
            _terminate: terminate,
            runtime,
        }
    }

    /// Runs `role` to completion on the runtime.
    pub fn block_on<F: Future>(&self, role: F) -> F::Output {
        self.runtime.block_on(role)
    }
}

/// An address nothing is listening on yet. The child's ports have to be named
/// before it starts, because the parent connects to them.
///
/// # Panics
///
/// Panics when no loopback port can be bound.
pub fn free_loopback_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a free port");
    listener
        .local_addr()
        .expect("the bound address")
        .to_string()
}
