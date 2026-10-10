//! A distributor on an ephemeral loopback port that judges each push by
//! per-tenant limit overrides and appends what it accepts to a recording sink.

use std::{net::SocketAddr, sync::Arc};

use krabka_metrics::{
    OverridesProvider,
    distributor::{DistributorState, serve},
};
use krabka_observability::server_security::ServerSecurity;

use crate::recording_sink::RecordingSink;

pub struct OverridesDistributor {
    addr: SocketAddr,
    pub sink: Arc<RecordingSink>,
}

impl OverridesDistributor {
    pub async fn boot(overrides: OverridesProvider) -> Self {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState::new(sink.clone()).with_overrides(overrides));
        let addr = serve(
            "127.0.0.1:0".parse().expect("socket addr"),
            state,
            &ServerSecurity::default(),
            std::future::pending(),
        )
        .await
        .expect("serve distributor");
        Self { addr, sink }
    }

    /// Pushes the snappy `remote_write` v1 `body` as `tenant`.
    pub async fn push(
        &self,
        client: &reqwest::Client,
        tenant: &str,
        body: Vec<u8>,
    ) -> reqwest::StatusCode {
        client
            .post(format!("http://{}/api/v1/push", self.addr))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .header("X-Scope-OrgID", tenant)
            .body(body)
            .send()
            .await
            .expect("send push")
            .status()
    }
}
