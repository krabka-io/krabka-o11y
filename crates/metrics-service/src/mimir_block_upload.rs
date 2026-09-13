use axum::{
    Json,
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use krabka_blockstore::escape_object_path_segment;
use krabka_metrics::{CompactionIndexManifest, MetricBlockKind};
use krabka_observability::server_security::Principal;
use object_store::{ObjectStoreExt as _, PutPayload, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};

use crate::MimirTenantAdminState;

const UPLOAD_PREFIX: &str = "mimir-block-uploads";
const MAX_META_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadMeta {
    ulid: String,
    min_time: i64,
    max_time: i64,
    version: i64,
    thanos: ThanosMeta,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ThanosMeta {
    files: Vec<UploadFile>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct UploadFile {
    rel_path: String,
    #[serde(default)]
    size_bytes: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredUploadState {
    result: UploadResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum UploadResult {
    Validating,
    Failed,
    Complete,
}

#[derive(Serialize)]
struct UploadStatus<'a> {
    result: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
}

pub(crate) async fn start_block_upload(
    State(state): State<MimirTenantAdminState>,
    Path(block): Path<String>,
    axum::Extension(principal): axum::Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (tenant, block) = match request_parameters(&headers, &principal, &block) {
        Ok(parameters) => parameters,
        Err(response) => return *response,
    };
    match load_state(&state, &tenant, &block).await {
        Ok(Some(saved)) => return state_conflict(saved.result),
        Ok(None) => {}
        Err(error) => return internal(error),
    }
    if body.len() > MAX_META_BYTES {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "The block metadata was too large",
        );
    }
    let meta = match parse_meta(&body, &block) {
        Ok(meta) => meta,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    let key = upload_object_key(&tenant, &block, "uploading-meta.json");
    match serde_json::to_vec(&meta) {
        Ok(bytes) => match state.store.put(&key, PutPayload::from(bytes)).await {
            Ok(_) => StatusCode::OK.into_response(),
            Err(error) => internal(error),
        },
        Err(error) => internal(error),
    }
}

pub(crate) async fn upload_block_file(
    State(state): State<MimirTenantAdminState>,
    Path(block): Path<String>,
    RawQuery(raw_query): RawQuery,
    axum::Extension(principal): axum::Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (tenant, block) = match request_parameters(&headers, &principal, &block) {
        Ok(parameters) => parameters,
        Err(response) => return *response,
    };
    match load_state(&state, &tenant, &block).await {
        Ok(Some(saved)) => return state_conflict(saved.result),
        Ok(None) => {}
        Err(error) => return internal(error),
    }
    let Some(path) = raw_query.as_deref().and_then(upload_path) else {
        return error(StatusCode::BAD_REQUEST, "missing or invalid file path");
    };
    if !valid_file_path(&path) {
        return error(StatusCode::BAD_REQUEST, format!("invalid path: {path:?}"));
    }
    if body.is_empty() {
        return error(StatusCode::BAD_REQUEST, "file cannot be empty");
    }
    let meta = match load_meta(&state, &tenant, &block).await {
        Ok(Some(meta)) => meta,
        Ok(None) => return error(StatusCode::NOT_FOUND, "block upload not started"),
        Err(error) => return internal(error),
    };
    let Some(file) = meta.thanos.files.iter().find(|file| file.rel_path == path) else {
        return error(StatusCode::BAD_REQUEST, "unexpected file");
    };
    if usize::try_from(file.size_bytes).ok() != Some(body.len()) {
        return error(StatusCode::BAD_REQUEST, "file size doesn't match meta.json");
    }
    let key = upload_object_key(&tenant, &block, &format!("files/{path}"));
    match state.store.put(&key, PutPayload::from(body)).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(error) => internal(error),
    }
}

pub(crate) async fn finish_block_upload(
    State(state): State<MimirTenantAdminState>,
    Path(block): Path<String>,
    axum::Extension(principal): axum::Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let (tenant, block) = match request_parameters(&headers, &principal, &block) {
        Ok(parameters) => parameters,
        Err(response) => return *response,
    };
    match load_state(&state, &tenant, &block).await {
        Ok(Some(StoredUploadState {
            result: UploadResult::Complete,
            ..
        })) => return state_conflict(UploadResult::Complete),
        Ok(Some(StoredUploadState {
            result: UploadResult::Failed,
            ..
        })) => return state_conflict(UploadResult::Failed),
        Ok(
            Some(StoredUploadState {
                result: UploadResult::Validating,
                ..
            })
            | None,
        ) => {}
        Err(error) => return internal(error),
    }
    let meta = match load_meta(&state, &tenant, &block).await {
        Ok(Some(meta)) => meta,
        Ok(None) => return error(StatusCode::NOT_FOUND, "block upload not started"),
        Err(error) => return internal(error),
    };
    if let Err(error) = save_state(
        &state,
        &tenant,
        &block,
        &StoredUploadState {
            result: UploadResult::Validating,
            error: None,
        },
    )
    .await
    {
        return internal(error);
    }

    let completion = complete_native_upload(&state, &tenant, &block, &meta).await;
    let saved = match completion {
        Ok(()) => StoredUploadState {
            result: UploadResult::Complete,
            error: None,
        },
        Err(message) => StoredUploadState {
            result: UploadResult::Failed,
            error: Some(message),
        },
    };
    if let Err(error) = save_state(&state, &tenant, &block, &saved).await {
        return internal(error);
    }
    if saved.result == UploadResult::Complete {
        state.query_store.invalidate().await;
    }
    StatusCode::OK.into_response()
}

pub(crate) async fn check_block_upload(
    State(state): State<MimirTenantAdminState>,
    Path(block): Path<String>,
    axum::Extension(principal): axum::Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let (tenant, block) = match request_parameters(&headers, &principal, &block) {
        Ok(parameters) => parameters,
        Err(response) => return *response,
    };
    let saved = match load_state(&state, &tenant, &block).await {
        Ok(saved) => saved,
        Err(error) => return internal(error),
    };
    if let Some(saved) = saved {
        let result = match saved.result {
            UploadResult::Validating => "validating",
            UploadResult::Failed => "failed",
            UploadResult::Complete => "complete",
        };
        return Json(UploadStatus {
            result,
            error: saved.error.as_deref(),
        })
        .into_response();
    }
    match load_meta(&state, &tenant, &block).await {
        Ok(Some(_)) => Json(UploadStatus {
            result: "uploading",
            error: None,
        })
        .into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "block doesn't exist"),
        Err(error) => internal(error),
    }
}

fn request_parameters(
    headers: &HeaderMap,
    principal: &Principal,
    block: &str,
) -> Result<(String, String), Box<Response>> {
    let tenant = krabka_metrics::authorized_tenant_from_headers(headers, principal)
        .map_err(|error| Box::new(error.into_response()))?;
    let block = canonical_block_id(block)
        .ok_or_else(|| Box::new(error(StatusCode::BAD_REQUEST, "invalid block ID")))?;
    Ok((tenant.as_str().to_owned(), block))
}

fn canonical_block_id(block: &str) -> Option<String> {
    let block = block.to_ascii_uppercase();
    (block.len() == 26
        && block.as_bytes().first().is_some_and(|first| *first <= b'7')
        && block
            .bytes()
            .all(|byte| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&byte)))
    .then_some(block)
}

fn parse_meta(body: &[u8], block: &str) -> Result<UploadMeta, String> {
    let mut meta = serde_json::from_slice::<UploadMeta>(body)
        .map_err(|_| "malformed request body".to_owned())?;
    block.clone_into(&mut meta.ulid);
    if meta.version != 1 {
        return Err("version must be 1".to_owned());
    }
    if meta.min_time < 0 || meta.max_time < meta.min_time {
        return Err(format!(
            "invalid minTime/maxTime: minTime={}, maxTime={}",
            meta.min_time, meta.max_time
        ));
    }
    if meta.thanos.files.is_empty() {
        return Err("missing thanos.files".to_owned());
    }
    for file in &meta.thanos.files {
        if file.rel_path != "meta.json" && !valid_file_path(&file.rel_path) {
            return Err(format!("file with invalid path: {}", file.rel_path));
        }
        if file.rel_path != "meta.json" && file.size_bytes <= 0 {
            return Err(format!("file with invalid size: {}", file.rel_path));
        }
    }
    Ok(meta)
}

fn valid_file_path(path: &str) -> bool {
    path == "index"
        || path.strip_prefix("chunks/").is_some_and(|suffix| {
            suffix.len() == 6 && suffix.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn upload_path(query: &str) -> Option<String> {
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(name, _)| name == "path")
        .map(|(_, value)| value.into_owned())
        .filter(|value| !value.is_empty())
}

async fn complete_native_upload(
    state: &MimirTenantAdminState,
    tenant: &str,
    block: &str,
    meta: &UploadMeta,
) -> Result<(), String> {
    let chunks = meta
        .thanos
        .files
        .iter()
        .filter(|file| file.rel_path.starts_with("chunks/"))
        .collect::<Vec<_>>();
    if !meta
        .thanos
        .files
        .iter()
        .any(|file| file.rel_path == "index")
        || chunks.len() != 1
    {
        return Err(
            "Krabka native block upload requires index and exactly one chunks/NNNNNN file"
                .to_owned(),
        );
    }
    let uploaded_index = upload_object_key(tenant, block, "files/index");
    let uploaded_block = upload_object_key(tenant, block, &format!("files/{}", chunks[0].rel_path));
    let index_bytes = object_bytes(state, &uploaded_index, "index").await?;
    let mut manifest = CompactionIndexManifest::decode(&index_bytes).map_err(|_| {
        "Prometheus TSDB index conversion is not yet supported; index must contain a Krabka compaction manifest"
            .to_owned()
    })?;
    if manifest.kind == MetricBlockKind::ClockReadings {
        return Err("clock-reading blocks are not queryable uploads".to_owned());
    }
    krabka_blockstore::read_block(state.store.clone(), uploaded_block.as_ref())
        .await
        .map_err(|error| format!("uploaded Krabka Parquet block is invalid: {error}"))?;
    if manifest.row_count == 0 {
        return Err("uploaded block manifest has no rows".to_owned());
    }

    let stem = native_object_stem(state, tenant, block);
    let final_block = ObjectPath::from(format!("{stem}.parquet"));
    let final_index = ObjectPath::from(format!("{stem}.index"));
    state
        .store
        .copy(&uploaded_block, &final_block)
        .await
        .map_err(|error| error.to_string())?;
    manifest.tenant = tenant.to_owned();
    manifest.block_key = final_block.to_string();
    manifest.index_key = final_index.to_string();
    state
        .store
        .put(
            &final_index,
            PutPayload::from(manifest.encode().map_err(|error| error.to_string())?),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn object_bytes(
    state: &MimirTenantAdminState,
    key: &ObjectPath,
    name: &str,
) -> Result<Bytes, String> {
    state
        .store
        .get(key)
        .await
        .map_err(|error| format!("missing uploaded {name}: {error}"))?
        .bytes()
        .await
        .map_err(|error| error.to_string())
}

async fn load_meta(
    state: &MimirTenantAdminState,
    tenant: &str,
    block: &str,
) -> Result<Option<UploadMeta>, String> {
    let key = upload_object_key(tenant, block, "uploading-meta.json");
    let bytes = match state.store.get(&key).await {
        Ok(object) => object.bytes().await.map_err(|error| error.to_string())?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| error.to_string())
}

async fn load_state(
    state: &MimirTenantAdminState,
    tenant: &str,
    block: &str,
) -> Result<Option<StoredUploadState>, String> {
    let key = upload_object_key(tenant, block, "state.json");
    let bytes = match state.store.get(&key).await {
        Ok(object) => object.bytes().await.map_err(|error| error.to_string())?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| error.to_string())
}

async fn save_state(
    state: &MimirTenantAdminState,
    tenant: &str,
    block: &str,
    saved: &StoredUploadState,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(saved).map_err(|error| error.to_string())?;
    state
        .store
        .put(
            &upload_object_key(tenant, block, "state.json"),
            PutPayload::from(bytes),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn upload_object_key(tenant: &str, block: &str, suffix: &str) -> ObjectPath {
    ObjectPath::from(format!(
        "{UPLOAD_PREFIX}/{}/{block}/{suffix}",
        escape_object_path_segment(tenant)
    ))
}

fn native_object_stem(state: &MimirTenantAdminState, tenant: &str, block: &str) -> String {
    let prefix = state.query_store.manifest_prefix.trim_end_matches('/');
    let suffix = format!("{}/uploaded/{block}", escape_object_path_segment(tenant));
    if prefix.is_empty() {
        suffix
    } else {
        format!("{prefix}/{suffix}")
    }
}

fn state_conflict(result: UploadResult) -> Response {
    match result {
        UploadResult::Complete => error(StatusCode::CONFLICT, "block already exists"),
        UploadResult::Validating => error(StatusCode::BAD_REQUEST, "block validation in progress"),
        UploadResult::Failed => error(StatusCode::BAD_REQUEST, "block validation failed"),
    }
}

fn error(status: StatusCode, message: impl std::fmt::Display) -> Response {
    (status, format!("{message}\n")).into_response()
}

fn internal(message: impl std::fmt::Display) -> Response {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use assert2::check;
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Method, Request},
    };
    use krabka_blockstore::{BlockWriter, LabelMatcher, Labels, MatchOp, TENANT_HEADER};
    use krabka_metrics::{
        FloatRow, ObjectStoreCompactionIndexSink, TenantCompactionRows,
        write_compacted_tenant_blocks,
    };
    use krabka_promql::MetricStore;
    use object_store::{ObjectStore, memory::InMemory};
    use tower::ServiceExt as _;

    use super::*;
    use crate::{RefreshingMetricBlockStore, WalHead, mimir_tenant_admin_router};

    const BLOCK: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    fn state(
        store: Arc<dyn ObjectStore>,
    ) -> (MimirTenantAdminState, Arc<RefreshingMetricBlockStore>) {
        let query_store = Arc::new(RefreshingMetricBlockStore::new(
            store.clone(),
            "memory:///".parse().unwrap(),
            "metrics",
            WalHead::new(),
        ));
        (
            MimirTenantAdminState::new(store, query_store.clone(), WalHead::new()),
            query_store,
        )
    }

    fn router(state: MimirTenantAdminState) -> Router {
        mimir_tenant_admin_router(state).layer(axum::Extension(Principal::Unauthenticated))
    }

    async fn request(router: &Router, method: Method, uri: &str, body: Bytes) -> Response {
        router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(TENANT_HEADER, "tenant-a")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn native_block_and_manifest() -> (Bytes, Bytes) {
        let seed: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let labels = [
            ("__name__".to_owned(), "uploaded_metric".to_owned()),
            ("job".to_owned(), "api".to_owned()),
        ]
        .into_iter()
        .collect::<Labels>();
        let fingerprint = labels.fingerprint();
        let rows = TenantCompactionRows {
            tenant: "tenant-a".to_owned(),
            series_labels: BTreeMap::from([(fingerprint, labels)]),
            float_rows: vec![FloatRow {
                fingerprint,
                timestamp_ms: 1_000,
                value: 42.0,
                start_timestamp_ms: None,
            }],
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
            clock_rows: Vec::new(),
        };
        let writes = write_compacted_tenant_blocks(
            &BlockWriter::new(seed.clone()),
            &ObjectStoreCompactionIndexSink::new(seed.clone()),
            &rows,
            0,
            0,
        )
        .await
        .unwrap();
        let manifest = &writes[0].manifest;
        let block = seed
            .get(&ObjectPath::from(manifest.block_key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        (block, Bytes::from(manifest.encode().unwrap()))
    }

    #[tokio::test]
    async fn block_upload_routes_survive_restart_and_publish_a_queryable_native_block() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (block, index) = native_block_and_manifest().await;
        let meta = serde_json::to_vec(&serde_json::json!({
            "ulid": BLOCK,
            "minTime": 1_000,
            "maxTime": 1_000,
            "version": 1,
            "thanos": {
                "files": [
                    {"rel_path": "index", "size_bytes": index.len()},
                    {"rel_path": "chunks/000001", "size_bytes": block.len()}
                ]
            }
        }))
        .unwrap();
        let (first_state, _) = state(store.clone());
        let first = router(first_state.clone());

        for _ in 0..2 {
            check!(
                request(
                    &first,
                    Method::POST,
                    &format!("/api/v1/upload/block/{BLOCK}/start"),
                    Bytes::from(meta.clone()),
                )
                .await
                .status()
                    == StatusCode::OK
            );
        }
        let uploading = request(
            &first,
            Method::GET,
            &format!("/api/v1/upload/block/{BLOCK}/check"),
            Bytes::new(),
        )
        .await;
        check!(
            to_bytes(uploading.into_body(), usize::MAX).await.unwrap()
                == Bytes::from_static(br#"{"result":"uploading"}"#)
        );
        for _ in 0..2 {
            check!(
                request(
                    &first,
                    Method::POST,
                    &format!("/api/v1/upload/block/{BLOCK}/files?path=index"),
                    index.clone(),
                )
                .await
                .status()
                    == StatusCode::OK
            );
        }
        check!(
            request(
                &first,
                Method::POST,
                &format!("/api/v1/upload/block/{BLOCK}/files?path=chunks%2F000001"),
                block,
            )
            .await
            .status()
                == StatusCode::OK
        );

        // A process can stop after recording validation but before publishing
        // the manifest. A retry on a new process resumes from durable files.
        save_state(
            &first_state,
            "tenant-a",
            BLOCK,
            &StoredUploadState {
                result: UploadResult::Validating,
                error: None,
            },
        )
        .await
        .unwrap();

        let (restarted_state, restarted_query) = state(store.clone());
        let restarted = router(restarted_state);
        let validating = request(
            &restarted,
            Method::GET,
            &format!("/api/v1/upload/block/{BLOCK}/check"),
            Bytes::new(),
        )
        .await;
        check!(
            to_bytes(validating.into_body(), usize::MAX).await.unwrap()
                == Bytes::from_static(br#"{"result":"validating"}"#)
        );
        check!(
            request(
                &restarted,
                Method::POST,
                &format!("/api/v1/upload/block/{BLOCK}/finish"),
                Bytes::new(),
            )
            .await
            .status()
                == StatusCode::OK
        );
        let complete = request(
            &restarted,
            Method::GET,
            &format!("/api/v1/upload/block/{BLOCK}/check"),
            Bytes::new(),
        )
        .await;
        check!(
            to_bytes(complete.into_body(), usize::MAX).await.unwrap()
                == Bytes::from_static(br#"{"result":"complete"}"#)
        );
        check!(
            request(
                &restarted,
                Method::POST,
                &format!("/api/v1/upload/block/{BLOCK}/finish"),
                Bytes::new(),
            )
            .await
            .status()
                == StatusCode::CONFLICT
        );

        let matchers = [LabelMatcher::new(
            "__name__",
            MatchOp::Eq,
            "uploaded_metric",
        )];
        let scan = restarted_query
            .scan("tenant-a", &matchers, 0, 2_000)
            .await
            .unwrap();
        check!(scan.float_table.is_some());
        check!(
            restarted_query
                .series("tenant-a", &matchers, 0, 2_000)
                .await
                .unwrap()
                .iter()
                .any(|labels| labels.get("__name__") == Some("uploaded_metric"))
        );
    }
}
