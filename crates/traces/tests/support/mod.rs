//! Minimal in-process broker helper for traces integration tests.

#![allow(dead_code)]

use krabka_broker::BrokerHandle;
use krabka_client_core::Client;
use krabka_traces::frontend::job::{BlockMetaInfo, RowGroupInfo};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use tempfile::TempDir;

#[path = "../../../integration/tests/support/test_broker.rs"]
mod shared_broker;

pub struct InProcess {
    pub broker: BrokerHandle,
    pub client: Client,
    pub bootstrap: String,
    pub _tempdir: TempDir,
}

pub async fn start() -> InProcess {
    let started = shared_broker::start_broker().await;
    let client = Client::builder()
        .bootstrap(&started.bootstrap)
        .client_id("krabka-traces-test")
        .build()
        .await
        .expect("client build");
    InProcess {
        broker: started.broker,
        client,
        bootstrap: started.bootstrap,
        _tempdir: started.tempdir,
    }
}

pub fn block_info(id: &str, start_ns: i64, end_ns: i64) -> BlockMetaInfo {
    BlockMetaInfo {
        block_id: id.to_string(),
        start_ns,
        end_ns,
        size: ByteSize::from_bytes(100),
        row_groups: vec![RowGroupInfo {
            index: 0,
            compressed: ByteSize::from_bytes(100),
        }],
    }
}

pub async fn spawn_axum(app: axum::Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}
