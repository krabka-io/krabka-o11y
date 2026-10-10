//! The object store a signal's lifecycle suite runs against.
//!
//! By default the store is in memory, and a suite behaves as it always has.
//! When `KRABKA_OBJECT_STORE_CONTRACT_URL` is set, the store is that provider
//! instead. Each test then gets a sub-prefix of its own below the contract
//! prefix, and writes the requests and bytes it cost as JSON. See
//! `docs/object_store_contract.md`.
//!
//! The four signal crates include this file with `#[path]`, so it depends only
//! on crates each of them already has.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use krabka_blockstore::{
    MeteredObjectStore, ObjectStoreMetrics, ObjectStoreOperation, object_store_cloud,
    object_store_endpoint_host,
};
use object_store::{
    ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path, prefix::PrefixStore,
};
use serde_json::json;
use url::Url;

/// The variable that selects a provider.
const CONTRACT_URL_VAR: &str = "KRABKA_OBJECT_STORE_CONTRACT_URL";

/// The prefix a destructive run must stay below.
const CONTRACT_PREFIX: &str = "krabka-contract/";

/// A step of the lifecycle that a test ran and checked.
#[derive(Clone, Copy, Debug)]
pub enum LifecycleStep {
    Flush,
    Query,
    Compaction,
    Retention,
    OrphanReconciliation,
    Restart,
}

impl LifecycleStep {
    fn as_str(self) -> &'static str {
        match self {
            Self::Flush => "flush",
            Self::Query => "query",
            Self::Compaction => "compaction",
            Self::Retention => "retention",
            Self::OrphanReconciliation => "orphan_reconciliation",
            Self::Restart => "restart",
        }
    }
}

enum Backing {
    Memory(Arc<InMemory>),
    Provider { url: Url, prefix: Path },
}

/// One test's object store, and what the test cost on it.
pub struct LifecycleStore {
    signal: &'static str,
    test: &'static str,
    backing: Backing,
    metrics: ObjectStoreMetrics,
    store: Arc<dyn ObjectStore>,
    restarts: usize,
    started: Instant,
}

impl LifecycleStore {
    /// Opens the store for `test` of `signal`.
    ///
    /// # Panics
    ///
    /// Panics when the contract URL does not parse, names a provider that is
    /// not configured, or points outside `krabka-contract/`.
    pub fn open(signal: &'static str, test: &'static str) -> Self {
        let backing = match std::env::var(CONTRACT_URL_VAR) {
            Ok(raw) => provider_backing(&raw, signal, test),
            Err(_) => Backing::Memory(Arc::new(InMemory::new())),
        };
        let metrics = ObjectStoreMetrics::unregistered();
        let store = connect(&backing, &metrics);
        Self {
            signal,
            test,
            backing,
            metrics,
            store,
            restarts: 0,
            started: Instant::now(),
        }
    }

    /// The store, as the roles of a running process share it.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// A new handle to the same data, as a restarted process builds one.
    ///
    /// A provider gets a new client. An in-memory store keeps its map, which
    /// is the only state a restart does not lose.
    pub fn restart(&mut self) -> Arc<dyn ObjectStore> {
        self.restarts += 1;
        self.store = connect(&self.backing, &self.metrics);
        self.store()
    }

    /// Deletes what the test wrote below a provider sub-prefix, and writes the
    /// cost report for `steps`.
    ///
    /// An in-memory store has nothing to report, and is dropped.
    ///
    /// # Panics
    ///
    /// Panics when the cleanup or the report fails.
    pub async fn finish(self, steps: &[LifecycleStep]) {
        let Backing::Provider { url, prefix } = &self.backing else {
            return;
        };
        let duration_seconds = self.started.elapsed().as_secs_f64();
        let mut operations = BTreeMap::new();
        let mut transferred_bytes = BTreeMap::new();
        let mut failures = BTreeMap::new();
        for operation in ObjectStoreOperation::all() {
            operations.insert(operation.as_str(), self.metrics.operations(operation));
            transferred_bytes.insert(
                operation.as_str(),
                self.metrics.transferred_bytes(operation),
            );
            failures.insert(operation.as_str(), self.metrics.failures(operation));
        }
        let report = json!({
            "schema_version": 1,
            "kind": "lifecycle",
            "commit": std::env::var("KRABKA_CONTRACT_COMMIT")
                .unwrap_or_else(|_| "unknown".into()),
            "provider": url.scheme(),
            "cloud": object_store_cloud(url),
            "bucket": url.host_str(),
            "endpoint_host": object_store_endpoint_host(),
            "signal": self.signal,
            "test": self.test,
            "prefix": prefix.as_ref(),
            "duration_seconds": duration_seconds,
            "restarts": self.restarts,
            "steps": steps.iter().map(|step| step.as_str()).collect::<Vec<_>>(),
            "requests_total": operations.values().sum::<u64>(),
            "transferred_bytes_total": transferred_bytes.values().sum::<u64>(),
            "operations": operations,
            "transferred_bytes": transferred_bytes,
            "failures": failures,
        });

        // The cleanup runs after the counters are read, so the report is the
        // test's cost alone.
        for location in objects_below(self.store.as_ref()).await {
            match self.store.delete(&location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => panic!("cannot delete {location}: {error}"),
            }
        }

        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        if let Some(dir) = report_dir() {
            let path = dir.join(format!(
                "object-store-lifecycle-{}-{}.json",
                self.signal, self.test
            ));
            std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap())
                .expect("the lifecycle report writes");
        }
    }
}

fn provider_backing(raw: &str, signal: &str, test: &str) -> Backing {
    let url = Url::parse(raw).expect("the contract URL parses");
    let (_, prefix) = object_store::parse_url_opts(&url, std::env::vars())
        .expect("provider configuration is valid before a lifecycle suite writes data");
    assert!(
        format!("{prefix}/").starts_with(CONTRACT_PREFIX),
        "refusing a destructive run outside a `{CONTRACT_PREFIX}` prefix"
    );
    let unique = format!(
        "{}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos(),
        std::process::id()
    );
    let prefix = prefix
        .join("lifecycle")
        .join(signal)
        .join(test)
        .join(unique.as_str());
    Backing::Provider { url, prefix }
}

fn connect(backing: &Backing, metrics: &ObjectStoreMetrics) -> Arc<dyn ObjectStore> {
    let store: Arc<dyn ObjectStore> = match backing {
        Backing::Memory(memory) => Arc::clone(memory) as Arc<dyn ObjectStore>,
        Backing::Provider { url, prefix } => {
            let (client, _) = object_store::parse_url_opts(url, std::env::vars())
                .expect("the provider client builds");
            Arc::new(PrefixStore::new(client, prefix.clone()))
        }
    };
    MeteredObjectStore::wrap(store, metrics.clone())
}

/// Every object in `store`, walked one delimiter level at a time.
///
/// The walk uses `list_with_delimiter` rather than a stream, so the signal
/// crates that include this file need no stream crate.
async fn objects_below(store: &dyn ObjectStore) -> Vec<Path> {
    let mut pending = vec![None];
    let mut objects = Vec::new();
    while let Some(prefix) = pending.pop() {
        let listing = store
            .list_with_delimiter(prefix.as_ref())
            .await
            .expect("the lifecycle sub-prefix lists");
        objects.extend(listing.objects.into_iter().map(|meta| meta.location));
        pending.extend(listing.common_prefixes.into_iter().map(Some));
    }
    objects
}

fn report_dir() -> Option<PathBuf> {
    std::env::var_os("KRABKA_OBJECT_STORE_LIFECYCLE_REPORT_DIR")
        .or_else(|| std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR"))
        .map(PathBuf::from)
}
