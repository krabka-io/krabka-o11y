//! Fixtures shared by the crate's unit-test modules.

#[path = "cpu_record.rs"]
mod cpu_record;

use std::{future::Future, net::SocketAddr, pin::Pin, sync::Arc};

use krabka_blockstore::{BlockIndex as _, BlockMeta, Labels, ObjectStoreMetrics, ProfileIndex};
use krabka_observability::server_security::ServerSecurity;
use krabka_pprof::{EngineOpts, FlameEngine, FlameGraph, NativeResolver, SymbolizeRequest};
use object_store::ObjectStore;

pub use self::cpu_record::{CPU_PROFILE_TYPE, CpuRecord, cpu_record};
use crate::{
    blockbuilder::build_block,
    cold_store::ColdProfileStore,
    wal::{ProfileRecord, WalSample, WalSymbolSet},
    wire::pb::otlp_profiles::{Function, Line, Location, ProfilesDictionary, Stack, ValueType},
};

/// Tenant `t`'s `api` flame graph over all time, as a cold store over
/// `store` and `index` answers it.
pub async fn cold_api_flamegraph(store: Arc<dyn ObjectStore>, index: ProfileIndex) -> FlameGraph {
    let cold = Arc::new(ColdProfileStore::new(store, Arc::new(index)));
    FlameEngine::new(cold, EngineOpts::default())
        .select_merge_stacktraces(
            "t",
            CPU_PROFILE_TYPE,
            r#"{service_name="api"}"#,
            0,
            i64::MAX,
            0,
        )
        .await
        .unwrap()
}

/// Writes `records`, all read at WAL offset `wal_offset`, as one profile
/// block for tenant `t`, partition 0.
pub async fn build_test_block(
    store: &Arc<dyn ObjectStore>,
    records: &[ProfileRecord],
    wal_offset: i64,
) -> BlockMeta {
    build_block(
        store,
        "t",
        0,
        records,
        (wal_offset, wal_offset),
        &ObjectStoreMetrics::unregistered(),
    )
    .await
    .unwrap()
    .remove(0)
}

/// An index for tenant `t` that knows the series of `records` and `blocks`.
pub fn index_with_series<'a>(
    records: impl IntoIterator<Item = &'a ProfileRecord>,
    blocks: &[&BlockMeta],
) -> ProfileIndex {
    let mut index = ProfileIndex::new();
    for rec in records {
        let labels = Labels::from_pairs(rec.labels.iter().cloned());
        index
            .add_series("t", labels.fingerprint(), &labels)
            .unwrap();
    }
    for block in blocks {
        index.add_block(block);
    }
    index
}

/// An OTLP dictionary with one function (`string_table[3]`) at one location
/// (address `0x40`, line 1) on one stack.
pub fn otlp_single_frame_dictionary(string_table: Vec<String>) -> ProfilesDictionary {
    ProfilesDictionary {
        string_table,
        function_table: vec![Function {
            name_strindex: 3,
            ..Default::default()
        }],
        location_table: vec![Location {
            address: 0x40,
            lines: vec![Line {
                function_index: 0,
                line: 1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        stack_table: vec![Stack {
            location_indices: vec![0],
        }],
        ..Default::default()
    }
}

/// The OTLP value type `string_table[1]`:`string_table[2]`, used as both the
/// sample type and the period type.
pub fn otlp_value_type() -> ValueType {
    ValueType {
        type_strindex: 1,
        unit_strindex: 2,
    }
}

/// The shutdown signal a test server waits on.
pub type Shutdown = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Starts a server on an ephemeral loopback port with default security.
///
/// `serve` is the role's `serve` entry point. The server runs until the
/// returned sender fires or is dropped.
pub async fn serve_on_loopback_with(
    serve: impl AsyncFnOnce(SocketAddr, &ServerSecurity, Shutdown) -> std::io::Result<SocketAddr>,
) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let bound = serve(
        "127.0.0.1:0".parse().unwrap(),
        &ServerSecurity::default(),
        Box::pin(async move {
            let _ = shutdown_rx.await;
        }),
    )
    .await
    .unwrap();
    (bound, shutdown_tx)
}

/// Checks that `resolver`, asked about an address in a file that does not
/// exist, answers with a frame named after the file and the address.
pub fn check_falls_back_to_address_frame(resolver: &dyn NativeResolver) {
    let out = resolver
        .symbolize(&SymbolizeRequest {
            build_id: String::new(),
            filename: "/missing/native".to_string(),
            address: 0x99,
        })
        .unwrap();

    assert2::assert!(out[0].function == "/missing/native+0x99");
    assert2::assert!(out[0].file == "/missing/native");
}
