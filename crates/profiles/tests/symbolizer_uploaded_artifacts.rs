#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use assert2::assert;
use krabka_blockstore::{BlockIndex, BlockLevel, BlockMeta, ProfileIndex};
#[cfg(target_os = "linux")]
use krabka_pprof::DebuginfodConfig;
use krabka_pprof::{ChainedResolver, LocationRec, MappingRec, MappingSymbolization, SymbolDb};
#[cfg(target_os = "linux")]
use krabka_profiles::symbolizer::offline_resolver_from_debuginfod_config;
use krabka_profiles::{metrics::ServiceMetrics, symbolizer::symbolize_blocks_once};
use object_store::{ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path};

#[derive(Debug, Default)]
struct CountingStore {
    inner: InMemory,
    artifact_gets: Mutex<HashMap<String, usize>>,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CountingStore")
    }
}

krabka_blockstore::delegate_object_store! {
    CountingStore => inner;
    forward [put_opts, put_multipart_opts, delete_stream, list, list_with_delimiter, copy_opts];

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if location.as_ref().starts_with("debug-info/") {
            *self.artifact_gets.lock().unwrap().entry(location.to_string()).or_default() += 1;
        }
        self.inner.get_opts(location, options).await
    }
}

fn pending_symbols(mappings: [(&str, &str); 2], addresses: u64) -> SymbolDb {
    let mut symbols = SymbolDb::new();
    for (build_id, filename) in mappings {
        let filename = symbols.intern_string(filename);
        let build_id = symbols.intern_string(build_id);
        let mapping_id = symbols.intern_mapping(MappingRec {
            memory_start: 0,
            memory_limit: u64::MAX,
            file_offset: 0,
            filename,
            build_id,
            symbolization: MappingSymbolization::default(),
        });
        for address in 0..addresses {
            symbols.intern_location(LocationRec {
                address,
                mapping_id,
                lines: Vec::new(),
            });
        }
    }
    symbols
}

async fn add_block(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    tenant: &str,
    name: &str,
    symbols: &SymbolDb,
) {
    let object_key = format!("blocks/{tenant}/{name}.parquet");
    store
        .put(
            &Path::from(format!("{object_key}.symdb")),
            symbols.encode().into(),
        )
        .await
        .unwrap();
    BlockIndex::add_block(
        index,
        &BlockMeta {
            tenant: tenant.to_string(),
            object_key: object_key.clone(),
            min_ts: 0,
            max_ts: 1,
            row_count: 1,
            fingerprints: Vec::new(),
            level: BlockLevel::INGESTED,
        },
    );
    index.add_profile_block(tenant, &object_key, vec![0]);
}

#[yare::parameterized(
    missing = { None },
    malformed = { Some(b"not an object".as_slice()) },
)]
#[test_macro(tokio::test)]
async fn unavailable_artifacts_are_looked_up_once_per_tenant_per_pass(artifact: Option<&[u8]>) {
    let counting = Arc::new(CountingStore::default());
    let store: Arc<dyn ObjectStore> = counting.clone();
    let mut index = ProfileIndex::new();
    let symbols = pending_symbols(
        [
            ("deadbeef", "/missing/native"),
            ("DEADBEEF", "/missing/other"),
        ],
        2048,
    );
    if let Some(artifact) = artifact {
        for tenant in ["tenant-a", "tenant-b"] {
            store
                .put(
                    &Path::from(format!("debug-info/{tenant}/deadbeef/exe")),
                    bytes::Bytes::copy_from_slice(artifact).into(),
                )
                .await
                .unwrap();
        }
    }
    for (tenant, name) in [
        ("tenant-a", "one"),
        ("tenant-a", "two"),
        ("tenant-b", "one"),
    ] {
        add_block(&store, &mut index, tenant, name, &symbols).await;
    }
    for pass in 1..=2 {
        assert2::assert!(
            symbolize_blocks_once(
                &store,
                &index,
                &ChainedResolver::default(),
                &ServiceMetrics::new()
            )
            .await
            .unwrap()
                == 0
        );
        assert2::assert!(
            *counting.artifact_gets.lock().unwrap()
                == HashMap::from([
                    ("debug-info/tenant-a/deadbeef/exe".to_string(), pass),
                    ("debug-info/tenant-b/deadbeef/exe".to_string(), pass),
                ])
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn upload_after_a_miss_is_visible_on_the_next_pass_only_to_its_tenant() {
    let counting = Arc::new(CountingStore::default());
    let store: Arc<dyn ObjectStore> = counting.clone();
    let mut index = ProfileIndex::new();
    let symbols = pending_symbols(
        [
            ("deadbeef", "/missing/native"),
            ("DEADBEEF", "/missing/other"),
        ],
        1,
    );
    for tenant in ["tenant-a", "tenant-b"] {
        add_block(&store, &mut index, tenant, "one", &symbols).await;
    }
    let resolver = ChainedResolver::default();
    let metrics = ServiceMetrics::new();
    assert!(
        symbolize_blocks_once(&store, &index, &resolver, &metrics)
            .await
            .unwrap()
            == 0
    );
    store
        .put(
            &Path::from("debug-info/tenant-a/deadbeef/exe"),
            std::fs::read(std::env::current_exe().unwrap())
                .unwrap()
                .into(),
        )
        .await
        .unwrap();
    assert!(
        symbolize_blocks_once(&store, &index, &resolver, &metrics)
            .await
            .unwrap()
            == 2
    );
    for (tenant, remaining) in [("tenant-a", 0), ("tenant-b", 2)] {
        let bytes = store
            .get(&Path::from(format!("blocks/{tenant}/one.parquet.symdb")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(
            SymbolDb::decode(&bytes)
                .unwrap()
                .pending_native_symbols()
                .len()
                == remaining
        );
    }
    assert!(
        *counting.artifact_gets.lock().unwrap()
            == HashMap::from([
                ("debug-info/tenant-a/deadbeef/exe".to_string(), 2),
                ("debug-info/tenant-b/deadbeef/exe".to_string(), 2),
            ])
    );
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_uploads_still_resolve_through_local_files_and_debuginfod() {
    let executable = std::env::current_exe().unwrap();
    let artifact = bytes::Bytes::from(std::fs::read(&executable).unwrap());
    let requests = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server_requests = requests.clone();
    let app = axum::Router::new().route(
        "/buildid/{build_id}/debuginfo",
        axum::routing::get(move || {
            let artifact = artifact.clone();
            let requests = server_requests.clone();
            async move {
                requests.fetch_add(1, Ordering::SeqCst);
                artifact
            }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    for (filename, expected_requests) in [(executable.to_str().unwrap(), 0), ("/missing/native", 2)]
    {
        let counting = Arc::new(CountingStore::default());
        let store: Arc<dyn ObjectStore> = counting.clone();
        let mut index = ProfileIndex::new();
        let symbols = pending_symbols([("deadbeef", filename), ("cafebabe", filename)], 2);
        assert!(symbols.pending_native_symbols().len() == 4);
        add_block(&store, &mut index, "tenant-a", "one", &symbols).await;
        add_block(&store, &mut index, "tenant-a", "two", &symbols).await;
        assert!(index.all_blocks().len() == 2);
        let url = url.clone();
        let updated = tokio::task::spawn_blocking(move || {
            let resolver =
                offline_resolver_from_debuginfod_config(vec![url], DebuginfodConfig::default())
                    .unwrap();
            futures::executor::block_on(symbolize_blocks_once(
                &store,
                &index,
                &resolver,
                &ServiceMetrics::new(),
            ))
            .unwrap()
        })
        .await
        .unwrap();
        assert!(
            updated == 8,
            "filename={filename}, requests={}",
            requests.load(Ordering::SeqCst)
        );
        assert!(requests.load(Ordering::SeqCst) == expected_requests);
        assert!(
            *counting.artifact_gets.lock().unwrap()
                == HashMap::from([
                    ("debug-info/tenant-a/deadbeef/exe".to_string(), 1),
                    ("debug-info/tenant-a/cafebabe/exe".to_string(), 1)
                ])
        );
    }
    server.abort();
}
