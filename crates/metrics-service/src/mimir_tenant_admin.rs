use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::TryStreamExt as _;
use krabka_blockstore::escape_object_path_segment;
use krabka_metrics::list_compaction_manifests;
use krabka_observability::server_security::{Principal, authorize_admin};
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload, path::Path};
use serde::{Deserialize, Serialize};

use crate::{
    MIMIR_TENANT_DELETION_PREFIX, RefreshingMetricBlockStore, WalHead,
    mimir_block_upload::{
        check_block_upload, finish_block_upload, start_block_upload, upload_block_file,
    },
};

#[derive(Clone)]
pub struct MimirTenantAdminState {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) query_store: Arc<RefreshingMetricBlockStore>,
    pub(crate) hot_store: WalHead,
}

impl MimirTenantAdminState {
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        query_store: Arc<RefreshingMetricBlockStore>,
        hot_store: WalHead,
    ) -> Self {
        Self {
            store,
            query_store,
            hot_store,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct TenantDeletionMarker {
    tenant_id: String,
    objects: Vec<String>,
}

#[derive(Serialize)]
struct TenantDeletionStatus<'a> {
    tenant_id: &'a str,
    blocks_deleted: bool,
}

pub fn mimir_tenant_admin_router(state: MimirTenantAdminState) -> Router {
    Router::new()
        .route(
            "/api/v1/upload/block/{block}/start",
            post(start_block_upload),
        )
        .route(
            "/api/v1/upload/block/{block}/files",
            post(upload_block_file).layer(DefaultBodyLimit::max(1 << 30)),
        )
        .route(
            "/api/v1/upload/block/{block}/finish",
            post(finish_block_upload),
        )
        .route(
            "/api/v1/upload/block/{block}/check",
            get(check_block_upload),
        )
        .route("/compactor/delete_tenant", post(delete_tenant))
        .route("/compactor/delete_tenant_status", get(delete_tenant_status))
        .with_state(state)
}

async fn delete_tenant(
    State(state): State<MimirTenantAdminState>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = authorize_admin(&principal) {
        return error.into_response();
    }
    let tenant = match krabka_metrics::authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let marker_key = tenant_deletion_marker_key(tenant.as_str());
    let marker = match new_marker(&state.store, tenant.as_str()).await {
        Ok(marker) => marker,
        Err(error) => return internal(error),
    };
    if let Err(error) = persist_marker(&state.store, &marker_key, &marker).await {
        return internal(error);
    }
    for object in &marker.objects {
        match state.store.delete(&Path::from(object.as_str())).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return internal(error),
        }
    }
    state.hot_store.delete_tenant(tenant.as_str());
    state.query_store.invalidate().await;
    StatusCode::OK.into_response()
}

async fn delete_tenant_status(
    State(state): State<MimirTenantAdminState>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let tenant = match krabka_metrics::authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let marker_key = tenant_deletion_marker_key(tenant.as_str());
    let blocks_deleted = match load_marker(&state.store, &marker_key).await {
        Ok(None) => true,
        Ok(Some(marker)) => match new_marker(&state.store, tenant.as_str()).await {
            Ok(current) => match marker_objects_absent(&state.store, &marker).await {
                Ok(absent) => absent && current.objects.is_empty(),
                Err(error) => return internal(error),
            },
            Err(error) => return internal(error),
        },
        Err(error) => return internal(error),
    };
    Json(TenantDeletionStatus {
        tenant_id: tenant.as_str(),
        blocks_deleted,
    })
    .into_response()
}

async fn new_marker(
    store: &Arc<dyn ObjectStore>,
    tenant: &str,
) -> Result<TenantDeletionMarker, String> {
    let manifests = list_compaction_manifests(store)
        .await
        .map_err(|error| error.to_string())?;
    let mut objects = manifests
        .into_iter()
        .filter(|manifest| manifest.tenant == tenant)
        .flat_map(|manifest| [manifest.index_key, manifest.block_key])
        .collect::<Vec<_>>();
    let upload_prefix = Path::from(format!(
        "mimir-block-uploads/{}",
        escape_object_path_segment(tenant)
    ));
    objects.extend(
        store
            .list(Some(&upload_prefix))
            .map_ok(|object| object.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| error.to_string())?,
    );
    objects.sort();
    objects.dedup();
    Ok(TenantDeletionMarker {
        tenant_id: tenant.to_owned(),
        objects,
    })
}

async fn persist_marker(
    store: &Arc<dyn ObjectStore>,
    key: &Path,
    marker: &TenantDeletionMarker,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(marker).map_err(|error| error.to_string())?;
    store
        .put(key, PutPayload::from(bytes))
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn load_marker(
    store: &Arc<dyn ObjectStore>,
    key: &Path,
) -> Result<Option<TenantDeletionMarker>, String> {
    let bytes = match store.get(key).await {
        Ok(object) => object.bytes().await.map_err(|error| error.to_string())?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| error.to_string())
}

async fn marker_objects_absent(
    store: &Arc<dyn ObjectStore>,
    marker: &TenantDeletionMarker,
) -> Result<bool, String> {
    for object in &marker.objects {
        match store.head(&Path::from(object.as_str())).await {
            Ok(_) => return Ok(false),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(true)
}

fn tenant_deletion_marker_key(tenant: &str) -> Path {
    Path::from(format!(
        "{MIMIR_TENANT_DELETION_PREFIX}/{}.json",
        escape_object_path_segment(tenant)
    ))
}

fn internal(error: impl std::fmt::Display) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}\n")).into_response()
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use axum::body::to_bytes;
    use krabka_blockstore::{BlockLevel, Labels, TENANT_HEADER};
    use krabka_metrics::{CompactionIndexManifest, MetricBlockKind};
    use krabka_promql::MetricStore;
    use object_store::memory::InMemory;

    use super::*;

    fn manifest() -> CompactionIndexManifest {
        CompactionIndexManifest {
            tenant: "tenant-a".to_owned(),
            kind: MetricBlockKind::Float,
            block_key: "metrics/a.parquet".to_owned(),
            index_key: "metrics/a.index".to_owned(),
            level: BlockLevel::INGESTED,
            first_offset: 0,
            last_offset: 0,
            row_count: 1,
            min_ts: 1_000,
            max_ts: 1_000,
            fingerprints: Vec::new(),
            series: Vec::new(),
        }
    }

    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(TENANT_HEADER, "tenant-a".parse().unwrap());
        headers
    }

    fn state(
        store: Arc<dyn ObjectStore>,
        head: WalHead,
    ) -> (MimirTenantAdminState, Arc<RefreshingMetricBlockStore>) {
        let query = Arc::new(RefreshingMetricBlockStore::new(
            Arc::clone(&store),
            "memory:///".parse().unwrap(),
            "metrics",
            head.clone(),
        ));
        (
            MimirTenantAdminState::new(store, Arc::clone(&query), head),
            query,
        )
    }

    #[tokio::test]
    async fn tenant_delete_is_idempotent_durable_and_hides_hot_data() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest = manifest();
        store
            .put(
                &Path::from(manifest.block_key.as_str()),
                PutPayload::from_static(b"block"),
            )
            .await
            .unwrap();
        store
            .put(
                &Path::from(manifest.index_key.as_str()),
                PutPayload::from(manifest.encode().unwrap()),
            )
            .await
            .unwrap();
        store
            .put(
                &Path::from("mimir-block-uploads/tenant-a/block/state.json"),
                PutPayload::from_static(b"{}"),
            )
            .await
            .unwrap();
        let head = WalHead::new();
        head.update(|head| {
            head.push_float(
                "tenant-a",
                [("__name__".to_owned(), "up".to_owned())]
                    .into_iter()
                    .collect::<Labels>(),
                1_000,
                1.0,
            );
        });
        let (admin_state, query) = state(Arc::clone(&store), head);

        for _ in 0..2 {
            let response = delete_tenant(
                State(admin_state.clone()),
                Extension(Principal::Unauthenticated),
                headers(),
            )
            .await;
            check!(response.status() == StatusCode::OK);
        }
        check!(
            query
                .series("tenant-a", &[], 0, 2_000)
                .await
                .unwrap()
                .is_empty()
        );
        check!(matches!(
            store.head(&Path::from("metrics/a.parquet")).await,
            Err(object_store::Error::NotFound { .. })
        ));
        check!(matches!(
            store
                .head(&Path::from("mimir-block-uploads/tenant-a/block/state.json"))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));

        let (restarted, _) = state(Arc::clone(&store), WalHead::new());
        let response = delete_tenant_status(
            State(restarted),
            Extension(Principal::Unauthenticated),
            headers(),
        )
        .await;
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let status: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        check!(
            status
                == serde_json::json!({
                    "tenant_id": "tenant-a",
                    "blocks_deleted": true
                })
        );
    }
}
