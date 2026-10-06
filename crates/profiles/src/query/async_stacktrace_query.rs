use object_store::{ObjectStoreExt, PutMode, PutOptions, path::Path};
use pb::querier::v1::{
    AsyncQueryResponse, AsyncQueryStatus, SelectMergeStacktracesRequest,
    SelectMergeStacktracesResponse,
};
use prost::Message;

use super::{Arc, Code, ConnectError, ProfileStore, QuerierState, TenantId, pb};

// Specs and answers share the tenant's admin store. A pending query can resume
// when polled through a fresh querier; repeated execution is safe because these
// queries are read-only. Local admission bounds background work per tenant.
pub(crate) async fn async_stacktrace_query<S: ProfileStore + 'static>(
    state: Arc<QuerierState<S>>,
    tenant: TenantId,
    mut request: SelectMergeStacktracesRequest,
) -> Result<SelectMergeStacktracesResponse, ConnectError> {
    let asynchronous = request
        .r#async
        .take()
        .ok_or_else(|| ConnectError::new(Code::InvalidArgument, "missing async request"))?;
    if !asynchronous.request_id.is_empty() {
        let id = asynchronous.request_id;
        uuid::Uuid::parse_str(&id).map_err(|error| {
            ConnectError::new(Code::Internal, format!("invalid request ID: {error}"))
        })?;
        // A successful immutable result wins over stale status writers.
        match state.admin_store.get(&completed_path(&tenant, &id)).await {
            Ok(object) => {
                let bytes = object
                    .bytes()
                    .await
                    .map_err(|error| storage_error(&error))?;
                return SelectMergeStacktracesResponse::decode(bytes)
                    .map_err(|error| ConnectError::new(Code::Internal, error.to_string()));
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(storage_error(&error)),
        }
        let path = result_path(&tenant, &id);
        let object = state
            .admin_store
            .get(&path)
            .await
            .map_err(|error| storage_error(&error))?;
        let bytes = object
            .bytes()
            .await
            .map_err(|error| storage_error(&error))?;
        let response = SelectMergeStacktracesResponse::decode(bytes)
            .map_err(|error| ConnectError::new(Code::Internal, error.to_string()))?;
        if response
            .r#async
            .as_ref()
            .is_some_and(|query| query.status == AsyncQueryStatus::InProgress as i32)
        {
            match state.admin_store.get(&request_path(&tenant, &id)).await {
                Ok(object) => {
                    let bytes = object
                        .bytes()
                        .await
                        .map_err(|error| storage_error(&error))?;
                    let spec = SelectMergeStacktracesRequest::decode(bytes)
                        .map_err(|error| ConnectError::new(Code::Internal, error.to_string()))?;
                    if reserve(&state, &tenant, &id).await {
                        spawn_query(Arc::clone(&state), tenant, id, spec);
                    }
                }
                Err(object_store::Error::NotFound { .. }) => {} // Submission is still storing its spec.
                Err(error) => return Err(storage_error(&error)),
            }
        }
        return Ok(response);
    }
    for _ in 0..16 {
        let id = uuid::Uuid::new_v4().to_string();
        if !reserve(&state, &tenant, &id).await {
            return Err(ConnectError::new(
                Code::ResourceExhausted,
                "async query concurrency limit reached",
            ));
        }
        let path = result_path(&tenant, &id);
        let pending = SelectMergeStacktracesResponse {
            r#async: Some(AsyncQueryResponse {
                request_id: id.clone(),
                status: AsyncQueryStatus::InProgress as i32,
                error_message: String::new(),
            }),
            ..Default::default()
        };
        let stored = state
            .admin_store
            .put_opts(
                &path,
                pending.encode_to_vec().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        if let Err(error) = stored {
            release(&state, &tenant, &id).await;
            if matches!(error, object_store::Error::AlreadyExists { .. }) {
                continue;
            }
            return Err(storage_error(&error));
        }
        if let Err(error) = state
            .admin_store
            .put(&request_path(&tenant, &id), request.encode_to_vec().into())
            .await
        {
            release(&state, &tenant, &id).await;
            state
                .admin_store
                .delete(&path)
                .await
                .map_err(|error| storage_error(&error))?;
            return Err(storage_error(&error));
        }
        spawn_query(state, tenant, id, request);
        return Ok(pending);
    }
    Err(ConnectError::new(
        Code::Internal,
        "could not reserve async request id",
    ))
}

fn result_path(tenant: &TenantId, id: &str) -> Path {
    Path::from(format!("profiles-admin/{tenant}/async/{id}.pb"))
}
fn completed_path(tenant: &TenantId, id: &str) -> Path {
    Path::from(format!("profiles-admin/{tenant}/async/{id}.success.pb"))
}
fn request_path(tenant: &TenantId, id: &str) -> Path {
    Path::from(format!("profiles-admin/{tenant}/async/{id}.request.pb"))
}
fn storage_error(error: &object_store::Error) -> ConnectError {
    ConnectError::new(
        if matches!(error, object_store::Error::NotFound { .. }) {
            Code::NotFound
        } else {
            Code::Internal
        },
        error.to_string(),
    )
}
async fn reserve<S: ProfileStore>(state: &QuerierState<S>, tenant: &TenantId, id: &str) -> bool {
    let maximum = state
        .overrides
        .for_tenant(tenant)
        .max_async_query_concurrency;
    let mut slots = state.async_query_slots.lock().await;
    let active = slots.entry(tenant.as_str().to_string()).or_default();
    if maximum == 0 || active.len() >= maximum || active.contains(id) {
        return false;
    }
    active.insert(id.to_string())
}
async fn release<S: ProfileStore>(state: &QuerierState<S>, tenant: &TenantId, id: &str) {
    let mut slots = state.async_query_slots.lock().await;
    if let Some(active) = slots.get_mut(tenant.as_str()) {
        active.remove(id);
        if active.is_empty() {
            slots.remove(tenant.as_str());
        }
    }
}
fn spawn_query<S: ProfileStore + 'static>(
    state: Arc<QuerierState<S>>,
    tenant: TenantId,
    id: String,
    request: SelectMergeStacktracesRequest,
) {
    let query_state = Arc::clone(&state);
    let query_tenant = tenant.clone();
    let task = tokio::spawn(async move {
        super::select_merge_stacktraces_inner::execute_stacktrace_query(
            &query_state,
            &query_tenant,
            request,
        )
        .await
    });
    tokio::spawn(async move {
        let mut response = match task.await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => failed_response(
                &id,
                error.message().unwrap_or("async query failed").to_string(),
            ),
            Err(error) => failed_response(&id, error.to_string()),
        };
        if response.r#async.is_none() {
            response.r#async = Some(AsyncQueryResponse {
                request_id: id.clone(),
                status: AsyncQueryStatus::Success as i32,
                error_message: String::new(),
            });
        }
        if response
            .r#async
            .as_ref()
            .is_some_and(|query| query.status == AsyncQueryStatus::Success as i32)
        {
            match state
                .admin_store
                .put_opts(
                    &completed_path(&tenant, &id),
                    response.encode_to_vec().into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(error) => {
                    tracing::error!(%error,"failed to anchor successful async query result");
                    release(&state, &tenant, &id).await;
                    return;
                }
            }
        }
        if let Err(error) = state
            .admin_store
            .put(&result_path(&tenant, &id), response.encode_to_vec().into())
            .await
        {
            // Keep the spec so the next poll can replay after transient storage failure.
            tracing::error!(%error, "failed to persist async profile query result");
        }
        release(&state, &tenant, &id).await;
    });
}
fn failed_response(id: &str, error_message: String) -> SelectMergeStacktracesResponse {
    SelectMergeStacktracesResponse {
        r#async: Some(AsyncQueryResponse {
            request_id: id.to_string(),
            status: AsyncQueryStatus::Failure as i32,
            error_message,
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::check;

    use super::*;

    fn request(id: &str) -> SelectMergeStacktracesRequest {
        SelectMergeStacktracesRequest {
            profile_type_id: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".into(),
            label_selector: "{}".into(),
            start: 1,
            end: 2,
            r#async: Some(pb::querier::v1::AsyncQueryRequest {
                request_id: id.into(),
                r#type: 1,
            }),
            ..Default::default()
        }
    }

    async fn completed(
        state: Arc<QuerierState>,
        tenant: &TenantId,
        id: &str,
    ) -> SelectMergeStacktracesResponse {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let response =
                    async_stacktrace_query(Arc::clone(&state), tenant.clone(), request(id))
                        .await
                        .unwrap();
                if response.r#async.as_ref().unwrap().status != AsyncQueryStatus::InProgress as i32
                {
                    return response;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn pending_specs_resume_on_fresh_state_and_completed_results_are_tenant_scoped() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let id = "04040404-0404-4404-8404-040404040404";
        let pending = SelectMergeStacktracesResponse {
            r#async: Some(AsyncQueryResponse {
                request_id: id.into(),
                status: AsyncQueryStatus::InProgress as i32,
                error_message: String::new(),
            }),
            ..Default::default()
        };
        store
            .put(&result_path(&tenant, id), pending.encode_to_vec().into())
            .await
            .unwrap();
        store
            .put(
                &request_path(&tenant, id),
                request("").encode_to_vec().into(),
            )
            .await
            .unwrap();
        let state = Arc::new(QuerierState::empty().with_admin_store(store.clone()));
        let response = completed(Arc::clone(&state), &tenant, id).await;
        check!(response.r#async.as_ref().unwrap().status == AsyncQueryStatus::Success as i32);
        check!(response.flamegraph.as_ref().unwrap().total == 0);
        // An older worker can publish a failure after another worker succeeded.
        // The previously completed answer must remain terminal and unchanged.
        store
            .put(
                &result_path(&tenant, id),
                failed_response(id, "stale worker failure".into())
                    .encode_to_vec()
                    .into(),
            )
            .await
            .unwrap();
        let restarted = Arc::new(QuerierState::empty().with_admin_store(store));
        let mut poll = request(id);
        poll.label_selector = "invalid ignored selector".into();
        poll.end = -10;
        let persisted = async_stacktrace_query(Arc::clone(&restarted), tenant.clone(), poll)
            .await
            .unwrap();
        check!(persisted == response);
        let other: TenantId = "tenant-b".parse().unwrap();
        let denied = async_stacktrace_query(restarted, other, request(id))
            .await
            .unwrap_err();
        check!(denied.code() == Code::NotFound);
        check!(state.async_query_slots.lock().await.is_empty());
    }

    #[tokio::test]
    async fn background_failure_is_recorded_and_concurrency_cap_blocks_submissions() {
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let state = Arc::new(QuerierState::empty());
        let mut invalid = request("");
        invalid.label_selector = "{malformed".into();
        let submission = async_stacktrace_query(Arc::clone(&state), tenant.clone(), invalid)
            .await
            .unwrap();
        let id = &submission.r#async.as_ref().unwrap().request_id;
        let response = completed(Arc::clone(&state), &tenant, id).await;
        check!(response.r#async.as_ref().unwrap().status == AsyncQueryStatus::Failure as i32);
        check!(!response.r#async.as_ref().unwrap().error_message.is_empty());
        check!(response.flamegraph.is_none());
        let disabled = Arc::new(QuerierState::new_with_limits(
            Arc::new(super::super::InMemoryProfileStore::new()),
            super::super::Limits {
                max_async_query_concurrency: 0,
                ..Default::default()
            },
        ));
        let rejected = async_stacktrace_query(disabled, tenant, request(""))
            .await
            .unwrap_err();
        check!(rejected.code() == Code::ResourceExhausted);
    }
}
