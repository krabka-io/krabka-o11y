//! Starts the in-process broker used by tests across workspace crates.

use krabka_broker::{Broker, BrokerConfig, BrokerHandle};

pub struct StartedBroker {
    pub broker: BrokerHandle,
    pub tempdir: tempfile::TempDir,
    pub bootstrap: String,
}

pub async fn start_broker() -> StartedBroker {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(tempdir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    StartedBroker { broker, tempdir, bootstrap }
}
