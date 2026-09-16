//! Destructive provider contract for the cloud object stores Krabka supports.
//!
//! The URL must point below `krabka-contract/`; every object below that prefix
//! is deleted. See `docs/object_store_contract.md` for credentials and usage.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Instant};

use assert2::assert;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use krabka_blockstore::{MeteredObjectStore, ObjectStoreMetrics, ObjectStoreOperation};
use object_store::{
    Error, ObjectStore, ObjectStoreExt as _, integration, path::Path, prefix::PrefixStore,
};
use serde_json::json;
use url::Url;

const OBJECTS_OVER_ONE_S3_PAGE: usize = 1_005;

async fn delete_all(store: &Arc<dyn ObjectStore>) {
    let paths = store
        .list(None)
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .expect("the contract prefix lists");
    store
        .delete_stream(stream::iter(paths.into_iter().map(Ok)).boxed())
        .try_collect::<Vec<_>>()
        .await
        .expect("the contract prefix deletes");
}

async fn wait_for_count(store: &Arc<dyn ObjectStore>, expected: usize) -> usize {
    for attempt in 1..=30 {
        let count = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("the contract prefix lists")
            .len();
        if count == expected {
            return attempt;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    panic!("the provider listing did not converge to {expected} objects within 30 seconds")
}

fn report_path() -> Option<PathBuf> {
    std::env::var_os("KRABKA_OBJECT_STORE_CONTRACT_REPORT")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR")
                .map(PathBuf::from)
                .map(|dir| dir.join("object-store-contract.json"))
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "destructive: requires a dedicated AWS, GCS, or Azure contract prefix"]
async fn supported_provider_satisfies_the_object_store_contract() {
    let raw_url = std::env::var("KRABKA_OBJECT_STORE_CONTRACT_URL")
        .expect("KRABKA_OBJECT_STORE_CONTRACT_URL must name a dedicated contract prefix");
    let url = Url::parse(&raw_url).expect("the contract URL parses");
    let (store, prefix) = object_store::parse_url_opts(&url, std::env::vars())
        .expect("provider configuration is valid before the contract writes data");
    assert!(
        prefix.as_ref().starts_with("krabka-contract/"),
        "refusing a destructive run outside a `krabka-contract/` prefix"
    );

    let metrics = ObjectStoreMetrics::unregistered();
    let prefixed: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(store, prefix));
    let store = MeteredObjectStore::wrap(prefixed, metrics.clone());
    let started = Instant::now();

    delete_all(&store).await;
    integration::put_get_delete_list(store.as_ref()).await;
    delete_all(&store).await;
    integration::get_opts(store.as_ref()).await;
    delete_all(&store).await;
    integration::put_opts(store.as_ref(), true).await;
    delete_all(&store).await;
    integration::rename_and_copy(&store).await;
    delete_all(&store).await;
    // Covers multipart invisibility before completion, completion, and abort.
    integration::stream_get(&store).await;
    delete_all(&store).await;
    integration::list_uses_directories_correctly(&store).await;
    delete_all(&store).await;
    integration::list_with_delimiter(&store).await;
    delete_all(&store).await;
    integration::list_with_offset_exclusivity(&store).await;
    delete_all(&store).await;
    assert!(matches!(
        integration::get_nonexistent_object(&store, None).await,
        Err(Error::NotFound { .. })
    ));

    stream::iter(0..OBJECTS_OVER_ONE_S3_PAGE)
        .map(|number| {
            let store = Arc::clone(&store);
            async move {
                store
                    .put(
                        &Path::from(format!("pagination/{number:04}")),
                        vec![u8::try_from(number % 251).unwrap()].into(),
                    )
                    .await
            }
        })
        .buffer_unordered(32)
        .try_collect::<Vec<_>>()
        .await
        .expect("more than one provider page writes");
    let listing_attempts = wait_for_count(&store, OBJECTS_OVER_ONE_S3_PAGE).await;
    delete_all(&store).await;
    let deletion_attempts = wait_for_count(&store, 0).await;
    assert!(metrics.operations(ObjectStoreOperation::Copy) > 0);
    assert!(metrics.operations(ObjectStoreOperation::PutMultipart) > 0);

    let mut operations = BTreeMap::new();
    let mut transferred_bytes = BTreeMap::new();
    for operation in ObjectStoreOperation::all() {
        operations.insert(operation.as_str(), metrics.operations(operation));
        transferred_bytes.insert(operation.as_str(), metrics.transferred_bytes(operation));
    }
    let report = json!({
        "schema_version": 1,
        "commit": std::env::var("KRABKA_CONTRACT_COMMIT").unwrap_or_else(|_| "unknown".into()),
        "provider": url.scheme(),
        "endpoint_host": url.host_str(),
        "duration_seconds": started.elapsed().as_secs_f64(),
        "pagination_objects": OBJECTS_OVER_ONE_S3_PAGE,
        "listing_convergence_attempts": listing_attempts,
        "deletion_convergence_attempts": deletion_attempts,
        "operations": operations,
        "transferred_bytes": transferred_bytes,
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if let Some(path) = report_path() {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap())
            .expect("the contract report writes");
    }
}
