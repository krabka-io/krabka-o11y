//! A bare broker running in the test process, with no topics provisioned.
//!
//! The `krabka-traces` and `krabka-profiles` binaries' unit tests reach this
//! file with `#[path]`, so it depends only on `krabka-broker` and `tempfile`.

use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use tempfile::TempDir;

/// A broker on a loopback port, logging to a temporary directory.
pub struct InProcessBroker {
    // Declared before the directory, so the broker stops before its files go.
    _handle: BrokerHandle,
    _directory: TempDir,
    /// The broker's listen address, as a bootstrap server.
    pub bootstrap: String,
}

impl InProcessBroker {
    /// Starts the broker with the test configuration.
    ///
    /// # Panics
    /// Panics when the temporary directory or the broker cannot be created.
    pub async fn start() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let handle = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = handle.listen_addr().to_string();
        Self {
            _handle: handle,
            _directory: directory,
            bootstrap,
        }
    }
}
