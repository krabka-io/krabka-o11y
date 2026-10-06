use std::{
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use object_store::{ObjectStoreExt, PutMode, path::Path};
use pb::querier::v1::{
    AsyncQueryResponse, AsyncQueryStatus, SelectMergeStacktracesRequest,
    SelectMergeStacktracesResponse,
};
use prost::Message;
use tokio_util::sync::CancellationToken;

use super::{Arc, Code, ConnectError, ProfileStore, QuerierState, TenantId, pb};

mod maintenance;
mod record;

/// Intervals for durable asynchronous query ownership and result retention.
#[derive(Clone, Debug)]
pub struct AsyncQueryPolicy {
    pub heartbeat_interval: Duration,
    pub lease_timeout: Duration,
    pub adoption_interval: Duration,
    pub cleanup_interval: Duration,
    pub retention: Duration,
    pub maximum_adoptions: u32,
}

impl Default for AsyncQueryPolicy {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(15),
            lease_timeout: Duration::from_secs(45),
            adoption_interval: Duration::from_secs(30),
            cleanup_interval: Duration::from_secs(300),
            retention: Duration::from_mins(30),
            maximum_adoptions: 3,
        }
    }
}

pub(crate) struct Runtime {
    pub owner: String,
    pub policy: AsyncQueryPolicy,
    pub stop: CancellationToken,
    started: AtomicBool,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    shutdown_lock: tokio::sync::Mutex<()>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            owner: uuid::Uuid::new_v4().to_string(),
            policy: AsyncQueryPolicy::default(),
            stop: CancellationToken::new(),
            started: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            shutdown_lock: tokio::sync::Mutex::new(()),
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

impl Runtime {
    fn spawn(&self, future: impl std::future::Future<Output = ()> + Send + 'static) -> bool {
        let mut tasks = self.tasks.lock().expect("async task registry poisoned");
        if self.stop.is_cancelled() {
            return false;
        }
        tasks.retain(|task| !task.is_finished());
        tasks.push(tokio::spawn(future));
        true
    }
}

pub(crate) fn start_maintenance<S: ProfileStore + 'static>(state: &Arc<QuerierState<S>>) {
    if !state.async_queries_enabled
        || state.query_architecture != super::PyroscopeQueryArchitecture::V2
        || state.async_runtime.started.swap(true, Ordering::AcqRel)
    {
        return;
    }
    let weak = Arc::downgrade(state);
    let stop = state.async_runtime.stop.clone();
    let policy = state.async_runtime.policy.clone();
    state.async_runtime.spawn(async move {
        let mut adoption = tokio::time::interval(policy.adoption_interval);
        let mut cleanup = tokio::time::interval(policy.cleanup_interval);
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => break,
                _ = adoption.tick() => {
                    let Some(state) = weak.upgrade() else { break };
                    tokio::select! {
                        biased;
                        () = stop.cancelled() => break,
                        result = maintenance::adopt(&state) => if let Err(error) = result { tracing::warn!(?error, "async profile adoption scan failed"); },
                    }
                }
                _ = cleanup.tick() => {
                    let Some(state) = weak.upgrade() else { break };
                    tokio::select! {
                        biased;
                        () = stop.cancelled() => break,
                        result = maintenance::cleanup(&state, record::now_ms()) => if let Err(error) = result { tracing::warn!(?error, "async profile retention scan failed"); },
                    }
                }
            }
        }
    });
}

pub(crate) async fn shutdown<S: ProfileStore>(state: &QuerierState<S>) {
    let _joining = state.async_runtime.shutdown_lock.lock().await;
    let tasks = {
        let mut tasks = state
            .async_runtime
            .tasks
            .lock()
            .expect("async task registry poisoned");
        state.async_runtime.stop.cancel();
        std::mem::take(&mut *tasks)
    };
    for task in tasks {
        if let Err(error) = task.await {
            tracing::warn!(?error, "async profile task stopped with error");
        }
    }
}

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
        uuid::Uuid::parse_str(&id)
            .map_err(|e| ConnectError::new(Code::Internal, format!("invalid request ID: {e}")))?;
        let (metadata, _) = record::load(&state, &tenant, &id).await?;
        if metadata.status != AsyncQueryStatus::InProgress as i32 {
            return committed_response(&state, &tenant, &id, &metadata).await;
        }
        // Pollers never adopt. Ownership changes only in the bounded scanner.
        if maintenance::expired(&state, &metadata, record::now_ms())
            && let Err(object_store::Error::NotFound { .. }) =
                state.admin_store.head(&request_path(&tenant, &id)).await
        {
            let message = "query orphaned: owner heartbeat expired and no request spec is available for adoption".to_string();
            if maintenance::fail_record(&state, metadata, message.clone()).await? {
                return Ok(failed_response(&id, message));
            }
            // A concurrent heartbeat or result publication wins the failed CAS.
            let (current, _) = record::load(&state, &tenant, &id).await?;
            return committed_response(&state, &tenant, &id, &current).await;
        }
        return Ok(pending_response(&id));
    }
    if state.async_runtime.stop.is_cancelled() {
        return Err(ConnectError::new(
            Code::Unavailable,
            "async querier is stopping",
        ));
    }
    let id = uuid::Uuid::new_v4().to_string();
    if !reserve(&state, &tenant, &id).await {
        return Err(ConnectError::new(
            Code::ResourceExhausted,
            "async query concurrency limit reached",
        ));
    }
    let stored = async {
        // The request precedes metadata publication, so a published record is recoverable.
        state
            .admin_store
            .put(&request_path(&tenant, &id), request.encode_to_vec().into())
            .await
            .map_err(|e| storage_error(&e))?;
        let metadata = record::Record {
            version: 1,
            generation: 0,
            tenant: tenant.as_str().into(),
            id: id.clone(),
            owner: state.async_runtime.owner.clone(),
            heartbeat_ms: record::now_ms(),
            adoptions: 0,
            status: AsyncQueryStatus::InProgress as i32,
            error: String::new(),
            result_generation: None,
        };
        if !record::write(&state, &metadata, PutMode::Create).await? {
            return Err(ConnectError::new(
                Code::Internal,
                "async request ID collision",
            ));
        }
        Ok(())
    }
    .await;
    if let Err(error) = stored {
        release(&state, &tenant, &id).await;
        return Err(error);
    }
    if !spawn_query(&state, tenant.clone(), id.clone(), request) {
        record::renew(&state, &tenant, &id, true).await?;
        release(&state, &tenant, &id).await;
        return Err(ConnectError::new(
            Code::Unavailable,
            "async querier is stopping",
        ));
    }
    Ok(pending_response(&id))
}

fn completed_path(tenant: &TenantId, id: &str, generation: u64) -> Path {
    Path::from(format!(
        "profiles-admin/{tenant}/async/{id}/results/{generation:020}.pb"
    ))
}

async fn committed_response<S: ProfileStore>(
    state: &QuerierState<S>,
    tenant: &TenantId,
    id: &str,
    metadata: &record::Record,
) -> Result<SelectMergeStacktracesResponse, ConnectError> {
    if metadata.status == AsyncQueryStatus::Failure as i32 {
        return Ok(failed_response(id, metadata.error.clone()));
    }
    if metadata.status != AsyncQueryStatus::Success as i32 {
        return Ok(pending_response(id));
    }
    let generation = metadata.result_generation.ok_or_else(|| {
        ConnectError::new(
            Code::Internal,
            "successful async query has no committed result",
        )
    })?;
    let object = state
        .admin_store
        .get(&completed_path(tenant, id, generation))
        .await
        .map_err(|error| storage_error(&error))?;
    SelectMergeStacktracesResponse::decode(
        object
            .bytes()
            .await
            .map_err(|error| storage_error(&error))?,
    )
    .map_err(|error| ConnectError::new(Code::Internal, error.to_string()))
}
fn request_path(tenant: &TenantId, id: &str) -> Path {
    Path::from(format!("profiles-admin/{tenant}/async/{id}/request.pb"))
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
    if maximum == 0
        || active.len() >= maximum
        || active.contains(id)
        || state.async_runtime.stop.is_cancelled()
    {
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
    state: &Arc<QuerierState<S>>,
    tenant: TenantId,
    id: String,
    request: SelectMergeStacktracesRequest,
) -> bool {
    let runtime_state = Arc::clone(state);
    state.async_runtime.spawn(async move {
        let query_state = Arc::clone(&runtime_state);
        let query_tenant = tenant.clone();
        let mut task = tokio::spawn(async move { super::select_merge_stacktraces_inner::execute_stacktrace_query(&query_state, &query_tenant, request).await });
        let mut heartbeat = tokio::time::interval(runtime_state.async_runtime.policy.heartbeat_interval);
        let response = loop {
            tokio::select! {
                biased;
                () = runtime_state.async_runtime.stop.cancelled() => {
                    task.abort(); let _ = task.await;
                    match tokio::time::timeout(runtime_state.async_runtime.policy.lease_timeout, relinquish_lease(&runtime_state, &tenant, &id)).await {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => tracing::warn!(?error, "failed to relinquish async query lease"),
                        Err(error) => tracing::warn!(%error, "lease relinquishment timed out; lease will expire"),
                    }
                    release(&runtime_state, &tenant, &id).await;
                    return;
                }
                result = &mut task => break match result { Ok(Ok(mut response)) => { response.r#async = pending_response(&id).r#async; if let Some(query) = &mut response.r#async { query.status = AsyncQueryStatus::Success as i32; } response }, Ok(Err(error)) => failed_response(&id, error.message().unwrap_or("async query failed").into()), Err(error) => failed_response(&id, error.to_string()) },
                // Finish the ownership CAS before relinquishing it. Filesystem
                // writes can outlive cancellation of their calling future.
                _ = heartbeat.tick() => match tokio::time::timeout(
                    runtime_state.async_runtime.policy.lease_timeout,
                    record::renew(&runtime_state, &tenant, &id, false),
                ).await {
                    Ok(Ok(true)) => {},
                    Ok(Ok(false)) => { task.abort(); let _ = task.await; release(&runtime_state, &tenant, &id).await; return; },
                    Ok(Err(error)) => tracing::warn!(?error, "failed to renew async query lease"),
                    Err(error) => tracing::warn!(%error, "async query heartbeat timed out"),
                }
            }
        };
        finish_query(&runtime_state, &tenant, &id, &response).await;
    })
}
// Once the backend has produced an outcome, shutdown joins its publication.
// Cancellation must not discard an answer or an in-flight ownership CAS.
async fn finish_query<S: ProfileStore>(
    state: &QuerierState<S>,
    tenant: &TenantId,
    id: &str,
    response: &SelectMergeStacktracesResponse,
) {
    let failed = match tokio::time::timeout(
        state.async_runtime.policy.lease_timeout,
        persist_result(state, tenant, id, response),
    )
    .await
    {
        Ok(Ok(())) => false,
        Ok(Err(error)) => {
            tracing::error!(?error, "failed to persist async query outcome");
            true
        }
        Err(error) => {
            tracing::error!(%error, "async query publication timed out");
            true
        }
    };
    if failed {
        match tokio::time::timeout(
            state.async_runtime.policy.lease_timeout,
            relinquish_lease(state, tenant, id),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(?error, "failed to relinquish unpublished async query");
            }
            Err(error) => tracing::warn!(%error, "unpublished async query lease will expire"),
        }
    }
    release(state, tenant, id).await;
}

// A timed-out backend write may still commit. Retry a lost generation CAS
// while we own the lease; adoption and terminal publication remain fences.
async fn relinquish_lease<S: ProfileStore>(
    state: &QuerierState<S>,
    tenant: &TenantId,
    id: &str,
) -> Result<(), ConnectError> {
    loop {
        if record::renew(state, tenant, id, true).await? {
            return Ok(());
        }
        let (current, _) = record::load(state, tenant, id).await?;
        if current.owner != state.async_runtime.owner
            || current.status != AsyncQueryStatus::InProgress as i32
        {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}

async fn persist_result<S: ProfileStore>(
    state: &QuerierState<S>,
    tenant: &TenantId,
    id: &str,
    response: &SelectMergeStacktracesResponse,
) -> Result<(), ConnectError> {
    let query = response
        .r#async
        .as_ref()
        .ok_or_else(|| ConnectError::new(Code::Internal, "missing async result metadata"))?;
    for _ in 0..16 {
        let (metadata, version) = record::load(state, tenant, id).await?;
        if publish_result(state, metadata, version, response).await? {
            return Ok(());
        }
        // A heartbeat may win the generation. Retry only while we still own
        // an in-progress query; an adoption or terminal outcome fences us.
        let (current, _) = record::load(state, tenant, id).await?;
        if current.owner != state.async_runtime.owner
            || current.status != AsyncQueryStatus::InProgress as i32
        {
            return Ok(());
        }
    }
    Err(ConnectError::new(
        Code::Unavailable,
        format!(
            "async result publication contended for {}",
            query.request_id
        ),
    ))
}

async fn publish_result<S: ProfileStore>(
    state: &QuerierState<S>,
    mut metadata: record::Record,
    version: object_store::UpdateVersion,
    response: &SelectMergeStacktracesResponse,
) -> Result<bool, ConnectError> {
    if metadata.owner != state.async_runtime.owner
        || metadata.status != AsyncQueryStatus::InProgress as i32
    {
        return Ok(false);
    }
    let tenant = TenantId::new(&metadata.tenant)
        .map_err(|error| ConnectError::new(Code::Internal, error.to_string()))?;
    let query = response
        .r#async
        .as_ref()
        .ok_or_else(|| ConnectError::new(Code::Internal, "missing async result metadata"))?;
    if query.request_id != metadata.id
        || (query.status != AsyncQueryStatus::Success as i32
            && query.status != AsyncQueryStatus::Failure as i32)
    {
        return Err(ConnectError::new(
            Code::Internal,
            "invalid async terminal result",
        ));
    }
    if query.status == AsyncQueryStatus::Success as i32 {
        let generation = metadata
            .generation
            .checked_add(1)
            .ok_or_else(|| ConnectError::new(Code::Internal, "async result generation overflow"))?;
        match state
            .admin_store
            .put_opts(
                &completed_path(&tenant, &metadata.id, generation),
                response.encode_to_vec().into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(storage_error(&error)),
        }
        metadata.result_generation = Some(generation);
    }
    metadata.status = query.status;
    metadata.error.clone_from(&query.error_message);
    metadata.heartbeat_ms = record::now_ms();
    // This immutable generation CAS is the only publication point. A staged
    // blob from a former owner is invisible unless its reference wins the CAS.
    record::write(state, &metadata, PutMode::Update(version)).await
}

fn pending_response(id: &str) -> SelectMergeStacktracesResponse {
    SelectMergeStacktracesResponse {
        r#async: Some(AsyncQueryResponse {
            request_id: id.into(),
            status: AsyncQueryStatus::InProgress as i32,
            error_message: String::new(),
        }),
        ..Default::default()
    }
}
fn failed_response(id: &str, error_message: String) -> SelectMergeStacktracesResponse {
    SelectMergeStacktracesResponse {
        r#async: Some(AsyncQueryResponse {
            request_id: id.into(),
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
        store
            .put(
                &request_path(&tenant, id),
                request("").encode_to_vec().into(),
            )
            .await
            .unwrap();
        let state = Arc::new(QuerierState::empty().with_admin_store(store.clone()));
        let metadata = record::Record {
            version: 1,
            generation: 0,
            tenant: tenant.as_str().into(),
            id: id.into(),
            owner: "dead-worker".into(),
            heartbeat_ms: 1,
            adoptions: 0,
            status: AsyncQueryStatus::InProgress as i32,
            error: String::new(),
            result_generation: None,
        };
        check!(
            record::write(&state, &metadata, PutMode::Create)
                .await
                .unwrap()
        );
        check!(
            async_stacktrace_query(Arc::clone(&state), tenant.clone(), request(id))
                .await
                .unwrap()
                .r#async
                .unwrap()
                .status
                == AsyncQueryStatus::InProgress as i32
        );
        maintenance::adopt(&state).await.unwrap();
        let response = completed(Arc::clone(&state), &tenant, id).await;
        check!(response.r#async.as_ref().unwrap().status == AsyncQueryStatus::Success as i32);
        check!(response.flamegraph.as_ref().unwrap().total == 0);
        // An older worker may attempt failure publication after another succeeded.
        // The previously completed answer must remain terminal and unchanged.
        record::terminal(
            &state,
            &tenant,
            id,
            AsyncQueryStatus::Failure as i32,
            "stale worker failure".into(),
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
    async fn publish_pending(
        state: &QuerierState,
        tenant: &TenantId,
        id: &str,
        heartbeat_ms: i64,
        adoptions: u32,
    ) {
        state
            .admin_store
            .put(
                &request_path(tenant, id),
                request("").encode_to_vec().into(),
            )
            .await
            .unwrap();
        let metadata = record::Record {
            version: 1,
            generation: 0,
            tenant: tenant.as_str().into(),
            id: id.into(),
            owner: "lost-owner".into(),
            heartbeat_ms,
            adoptions,
            status: AsyncQueryStatus::InProgress as i32,
            error: String::new(),
            result_generation: None,
        };
        check!(
            record::write(state, &metadata, PutMode::Create)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn competing_scanners_claim_once_and_fresh_leases_are_not_adopted() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let first = Arc::new(QuerierState::empty().with_admin_store(store.clone()));
        let second = Arc::new(QuerierState::empty().with_admin_store(store));
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let stale = "11111111-1111-4111-8111-111111111111";
        let fresh = "22222222-2222-4222-8222-222222222222";
        publish_pending(&first, &tenant, stale, 1, 0).await;
        publish_pending(&first, &tenant, fresh, record::now_ms(), 0).await;
        let (a, b) = tokio::join!(maintenance::adopt(&first), maintenance::adopt(&second));
        a.unwrap();
        b.unwrap();
        let completed = completed(Arc::clone(&first), &tenant, stale).await;
        check!(completed.r#async.unwrap().status == AsyncQueryStatus::Success as i32);
        let (claimed, _) = record::load(&first, &tenant, stale).await.unwrap();
        check!(claimed.adoptions == 1);
        check!(
            claimed.owner == first.async_runtime.owner
                || claimed.owner == second.async_runtime.owner
        );
        let (untouched, _) = record::load(&first, &tenant, fresh).await.unwrap();
        check!(untouched.adoptions == 0);
        check!(untouched.owner == "lost-owner");
        // A stale owner cannot renew or overwrite the winning worker's result.
        check!(!record::renew(&first, &tenant, fresh, false).await.unwrap());
        record::terminal(
            &first,
            &tenant,
            stale,
            AsyncQueryStatus::Failure as i32,
            "late failure".into(),
        )
        .await
        .unwrap();
        check!(
            record::load(&first, &tenant, stale).await.unwrap().0.status
                == AsyncQueryStatus::Success as i32
        );
        first.shutdown_async_queries().await;
        second.shutdown_async_queries().await;
    }

    #[tokio::test]
    async fn adoption_limit_missing_specs_and_corrupt_specs_fail_without_execution() {
        let state = Arc::new(QuerierState::empty());
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let ids = [
            "33333333-3333-4333-8333-333333333333",
            "44444444-4444-4444-8444-444444444444",
            "55555555-5555-4555-8555-555555555555",
        ];
        for (index, id) in ids.iter().enumerate() {
            publish_pending(&state, &tenant, id, 1, if index == 0 { 3 } else { 0 }).await;
        }
        state
            .admin_store
            .delete(&request_path(&tenant, ids[1]))
            .await
            .unwrap();
        state
            .admin_store
            .put(&request_path(&tenant, ids[2]), vec![0xff].into())
            .await
            .unwrap();
        maintenance::adopt(&state).await.unwrap();
        for id in ids {
            let result = async_stacktrace_query(Arc::clone(&state), tenant.clone(), request(id))
                .await
                .unwrap();
            check!(result.r#async.as_ref().unwrap().status == AsyncQueryStatus::Failure as i32);
            check!(!result.r#async.unwrap().error_message.is_empty());
            check!(result.flamegraph.is_none());
            check!(matches!(
                state
                    .admin_store
                    .head(&completed_path(&tenant, id, 1))
                    .await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
        check!(state.async_query_slots.lock().await.is_empty());
    }

    #[tokio::test]
    async fn adopted_owner_fences_a_result_staged_after_the_former_owner_read_metadata() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let first = Arc::new(QuerierState::empty().with_admin_store(store.clone()));
        let second = Arc::new(QuerierState::empty().with_admin_store(store.clone()));
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let id = "88888888-8888-4888-8888-888888888888";
        publish_pending(&first, &tenant, id, 1, 0).await;
        let (mut initial, version) = record::load(&first, &tenant, id).await.unwrap();
        initial.owner.clone_from(&first.async_runtime.owner);
        check!(
            record::write(&first, &initial, PutMode::Update(version))
                .await
                .unwrap()
        );
        // Force the TOCTOU interleaving: A reads, B takes ownership, then A
        // stages its answer and tries to publish the old metadata generation.
        let (observed_by_first, first_version) = record::load(&first, &tenant, id).await.unwrap();
        let (mut adopted, second_version) = record::load(&second, &tenant, id).await.unwrap();
        adopted.owner.clone_from(&second.async_runtime.owner);
        adopted.adoptions += 1;
        adopted.heartbeat_ms = record::now_ms();
        check!(
            record::write(&second, &adopted, PutMode::Update(second_version))
                .await
                .unwrap()
        );
        let mut stale_answer = pending_response(id);
        stale_answer.r#async.as_mut().unwrap().status = AsyncQueryStatus::Success as i32;
        stale_answer
            .flamegraph
            .get_or_insert_with(Default::default)
            .total = 11;
        let staged_generation = observed_by_first.generation + 1;
        check!(
            !publish_result(&first, observed_by_first, first_version, &stale_answer)
                .await
                .unwrap()
        );
        check!(
            store
                .head(&completed_path(&tenant, id, staged_generation))
                .await
                .is_ok()
        );
        for owner in [&first, &second] {
            let response = async_stacktrace_query(Arc::clone(owner), tenant.clone(), request(id))
                .await
                .unwrap();
            check!(response.r#async.unwrap().status == AsyncQueryStatus::InProgress as i32);
            check!(response.flamegraph.is_none());
        }
        let (current, _) = record::load(&second, &tenant, id).await.unwrap();
        check!(current.owner == second.async_runtime.owner);
        check!(current.result_generation.is_none());
        let mut winning_answer = stale_answer.clone();
        winning_answer.flamegraph.as_mut().unwrap().total = 22;
        persist_result(&second, &tenant, id, &winning_answer)
            .await
            .unwrap();
        persist_result(&first, &tenant, id, &stale_answer)
            .await
            .unwrap();
        let restarted = Arc::new(QuerierState::empty().with_admin_store(store));
        for owner in [&first, &second, &restarted] {
            let response = async_stacktrace_query(Arc::clone(owner), tenant.clone(), request(id))
                .await
                .unwrap();
            check!(response == winning_answer);
        }
        let (published, _) = record::load(&second, &tenant, id).await.unwrap();
        check!(published.result_generation == Some(staged_generation + 1));
    }

    // Gate the actual conditional filesystem put after the caller has checked
    // its expiry fence. A second state uses the same backend without this gate.
    #[derive(Debug)]
    struct SuspendedPutStore {
        inner: Arc<dyn object_store::ObjectStore>,
        location: Path,
        armed: std::sync::atomic::AtomicBool,
        fail_put: bool,
        entered: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }

    impl std::fmt::Display for SuspendedPutStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("suspended conditional put")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for SuspendedPutStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if location == &self.location
                && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                self.entered.notify_one();
                self.resume.notified().await;
                if self.fail_put {
                    return Err(object_store::Error::Generic {
                        store: "suspended conditional put",
                        source: Box::new(std::io::Error::other("injected publication failure")),
                    });
                }
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn expiry_fences_uploads_suspended_before_and_after_the_generation_seal_check() {
        use futures::TryStreamExt as _;
        for gate_metadata in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let backend: Arc<dyn object_store::ObjectStore> = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
            );
            let tenant: TenantId = "tenant-a".parse().unwrap();
            let id = "88888888-8888-4888-8888-888888888888";
            let gate = Arc::new(SuspendedPutStore {
                inner: Arc::clone(&backend),
                location: if gate_metadata {
                    record::metadata_path(&tenant, id, 2)
                } else {
                    completed_path(&tenant, id, 2)
                },
                armed: std::sync::atomic::AtomicBool::new(true),
                fail_put: false,
                entered: tokio::sync::Notify::new(),
                resume: tokio::sync::Notify::new(),
            });
            let first = Arc::new(QuerierState::empty().with_admin_store(gate.clone()));
            let second = Arc::new(QuerierState::empty().with_admin_store(Arc::clone(&backend)));
            publish_pending(&first, &tenant, id, 1, 0).await;
            let (mut initial, version) = record::load(&first, &tenant, id).await.unwrap();
            initial.owner.clone_from(&first.async_runtime.owner);
            check!(
                record::write(&first, &initial, PutMode::Update(version))
                    .await
                    .unwrap()
            );
            let (saved, version) = record::load(&first, &tenant, id).await.unwrap();
            let mut stale_answer = pending_response(id);
            stale_answer.r#async.as_mut().unwrap().status = AsyncQueryStatus::Success as i32;
            stale_answer
                .flamegraph
                .get_or_insert_with(Default::default)
                .total = 11;
            let late_upload = tokio::spawn({
                let first = Arc::clone(&first);
                async move { publish_result(&first, saved, version, &stale_answer).await }
            });
            tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
                .await
                .unwrap();
            let (mut adopted, version) = record::load(&second, &tenant, id).await.unwrap();
            adopted.owner.clone_from(&second.async_runtime.owner);
            adopted.adoptions += 1;
            check!(
                record::write(&second, &adopted, PutMode::Update(version))
                    .await
                    .unwrap()
            );
            let mut winning_answer = pending_response(id);
            winning_answer.r#async.as_mut().unwrap().status = AsyncQueryStatus::Success as i32;
            winning_answer
                .flamegraph
                .get_or_insert_with(Default::default)
                .total = 22;
            persist_result(&second, &tenant, id, &winning_answer)
                .await
                .unwrap();
            check!(
                async_stacktrace_query(Arc::clone(&second), tenant.clone(), request(id))
                    .await
                    .unwrap()
                    == winning_answer
            );
            let expiry = record::load(&second, &tenant, id)
                .await
                .unwrap()
                .0
                .heartbeat_ms
                + 1_800_001;
            check!(
                maintenance::cleanup(&second, expiry).await.unwrap()
                    == if gate_metadata { 7 } else { 6 }
            );
            let seal_path = record::expiry_path(&tenant, id);
            let seal_bytes = backend
                .get(&seal_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            gate.resume.notify_one();
            check!(
                !tokio::time::timeout(Duration::from_secs(5), late_upload)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
            );
            // Both old processes and a reopened filesystem reject the stale
            // answer, even though the formerly occupied key now exists again.
            let reopened = Arc::new(QuerierState::empty().with_admin_store(Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
            )));
            for state in [&first, &second, &reopened] {
                check!(
                    async_stacktrace_query(Arc::clone(state), tenant.clone(), request(id))
                        .await
                        .unwrap_err()
                        .code()
                        == Code::NotFound
                );
                check!(!record::renew(state, &tenant, id, false).await.unwrap());
            }
            check!(backend.head(&gate.location).await.is_ok());
            // A fresh conditional create cannot reuse the expired query ID.
            check!(
                !record::write(&second, &initial, PutMode::Create)
                    .await
                    .unwrap()
            );
            // Subsequent cleanup removes even fresh resumed uploads, but never
            // removes or replaces the fence. No old generation can reopen it.
            check!(maintenance::cleanup(&second, expiry).await.unwrap() == 1);
            let remaining = backend
                .list(Some(&Path::from(format!(
                    "profiles-admin/{tenant}/async/{id}"
                ))))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .into_iter()
                .map(|object| object.location)
                .collect::<Vec<_>>();
            check!(remaining == vec![seal_path.clone()]);
            check!(
                backend
                    .get(&seal_path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    == seal_bytes
            );
            check!(maintenance::cleanup(&second, expiry).await.unwrap() == 0);
        }
    }

    #[tokio::test]
    async fn corrupt_or_unsupported_expiry_seals_reject_reads_writes_and_cleanup() {
        use futures::TryStreamExt as _;
        for bytes in [
            b"{".to_vec(),
            br#"{"version":2,"tenant":"tenant-a","id":"99999999-9999-4999-8999-999999999999","expired_ms":1}"#.to_vec(),
            br#"{"version":1,"tenant":"tenant-b","id":"99999999-9999-4999-8999-999999999999","expired_ms":1}"#.to_vec(),
        ] {
            let state = QuerierState::empty();
            let tenant: TenantId = "tenant-a".parse().unwrap();
            let id = "99999999-9999-4999-8999-999999999999";
            publish_pending(&state, &tenant, id, 1, 0).await;
            let (mut metadata, version) = record::load(&state, &tenant, id).await.unwrap();
            metadata.status = AsyncQueryStatus::Success as i32;
            let seal = record::expiry_path(&tenant, id);
            state.admin_store.put(&seal, bytes.clone().into()).await.unwrap();
            check!(record::load(&state, &tenant, id).await.unwrap_err().code() == Code::Internal);
            check!(record::write(&state, &metadata, PutMode::Update(version)).await.unwrap_err().code() == Code::Internal);
            check!(maintenance::cleanup(&state, record::now_ms() + 1_800_001).await.unwrap() == 0);
            check!(state.admin_store.get(&seal).await.unwrap().bytes().await.unwrap().as_ref() == bytes.as_slice());
            check!(state.admin_store.list(None).try_collect::<Vec<_>>().await.unwrap().len() == 3);
        }
    }

    #[tokio::test]
    async fn retention_preserves_recovery_files_until_terminal_and_then_expires_the_group() {
        let state = QuerierState::empty();
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let id = "66666666-6666-4666-8666-666666666666";
        publish_pending(&state, &tenant, id, 1, 0).await;
        let unrelated = Path::from("profiles-admin/tenant-b/settings.json");
        state
            .admin_store
            .put(&unrelated, b"{}".to_vec().into())
            .await
            .unwrap();
        let future = record::now_ms() + 1_800_001;
        check!(maintenance::cleanup(&state, future).await.unwrap() == 0);
        check!(
            state
                .admin_store
                .head(&request_path(&tenant, id))
                .await
                .is_ok()
        );
        let (expired, _) = record::load(&state, &tenant, id).await.unwrap();
        check!(
            maintenance::fail_record(&state, expired, "adoption exhausted".into())
                .await
                .unwrap()
        );
        // Retention starts at the terminal transition, not the old lease or
        // request creation time; all generations expire as one query group.
        check!(
            maintenance::cleanup(&state, record::now_ms())
                .await
                .unwrap()
                == 0
        );
        let terminal_expiry = record::load(&state, &tenant, id)
            .await
            .unwrap()
            .0
            .heartbeat_ms
            + 1_800_001;
        check!(maintenance::cleanup(&state, terminal_expiry).await.unwrap() == 3);
        check!(state.admin_store.head(&unrelated).await.is_ok());
        check!(record::load(&state, &tenant, id).await.unwrap_err().code() == Code::NotFound);
    }

    async fn owned_pending(state: &QuerierState, tenant: &TenantId, id: &str) -> record::Record {
        publish_pending(state, tenant, id, record::now_ms(), 0).await;
        let (mut metadata, version) = record::load(state, tenant, id).await.unwrap();
        metadata.owner.clone_from(&state.async_runtime.owner);
        check!(
            record::write(state, &metadata, PutMode::Update(version))
                .await
                .unwrap()
        );
        record::load(state, tenant, id).await.unwrap().0
    }

    fn prepared_success(id: &str) -> SelectMergeStacktracesResponse {
        let mut response = pending_response(id);
        response.r#async.as_mut().unwrap().status = AsyncQueryStatus::Success as i32;
        response
            .flamegraph
            .get_or_insert_with(Default::default)
            .total = 11;
        response
    }

    #[tokio::test]
    async fn prepared_outcomes_are_published_after_shutdown_is_cancelled() {
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        for response in [
            prepared_success(id),
            failed_response(id, "backend failed".into()),
        ] {
            let backend = Arc::new(object_store::memory::InMemory::new());
            let state = Arc::new(QuerierState::empty().with_admin_store(backend.clone()));
            owned_pending(&state, &tenant, id).await;
            check!(reserve(&state, &tenant, id).await);
            state.async_runtime.stop.cancel();
            finish_query(&state, &tenant, id, &response).await;
            check!(state.async_query_slots.lock().await.is_empty());
            let reopened = Arc::new(QuerierState::empty().with_admin_store(backend));
            check!(
                async_stacktrace_query(Arc::clone(&reopened), tenant.clone(), request(id))
                    .await
                    .unwrap()
                    == response
            );
            let terminal = record::load(&reopened, &tenant, id).await.unwrap().0;
            check!(terminal.generation == 2);
            check!(terminal.adoptions == 0);
            maintenance::adopt(&reopened).await.unwrap();
            check!(record::load(&reopened, &tenant, id).await.unwrap().0 == terminal);
            reopened.shutdown_async_queries().await;
        }
    }

    #[tokio::test]
    async fn shutdown_joins_suspended_publication_and_relinquishes_failed_publication() {
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
        for (gate_metadata, fail_put) in [(false, false), (true, false), (false, true)] {
            let directory = tempfile::tempdir().unwrap();
            let backend: Arc<dyn object_store::ObjectStore> = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
            );
            let gate = Arc::new(SuspendedPutStore {
                inner: Arc::clone(&backend),
                location: if gate_metadata {
                    record::metadata_path(&tenant, id, 2)
                } else {
                    completed_path(&tenant, id, 2)
                },
                armed: std::sync::atomic::AtomicBool::new(true),
                fail_put,
                entered: tokio::sync::Notify::new(),
                resume: tokio::sync::Notify::new(),
            });
            let state = Arc::new(QuerierState::empty().with_admin_store(gate.clone()));
            let initial = owned_pending(&state, &tenant, id).await;
            check!(reserve(&state, &tenant, id).await);
            let expected = prepared_success(id);
            check!(state.async_runtime.spawn({
                let state = Arc::clone(&state);
                let tenant = tenant.clone();
                let response = expected.clone();
                async move { finish_query(&state, &tenant, id, &response).await }
            }));
            tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
                .await
                .unwrap();
            // A staged blob is invisible until the terminal generation commits.
            check!(
                async_stacktrace_query(Arc::clone(&state), tenant.clone(), request(id))
                    .await
                    .unwrap()
                    == pending_response(id)
            );
            let joining = tokio::spawn({
                let state = Arc::clone(&state);
                async move { state.shutdown_async_queries().await }
            });
            state.async_runtime.stop.cancelled().await;
            gate.resume.notify_one();
            tokio::time::timeout(Duration::from_secs(5), joining)
                .await
                .unwrap()
                .unwrap();
            check!(state.async_query_slots.lock().await.is_empty());
            let reopened = Arc::new(QuerierState::empty().with_admin_store(Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
            )));
            if fail_put {
                let mut relinquished = initial;
                relinquished.generation = 2;
                relinquished.heartbeat_ms = 1;
                check!(record::load(&reopened, &tenant, id).await.unwrap().0 == relinquished);
                check!(
                    async_stacktrace_query(Arc::clone(&reopened), tenant.clone(), request(id))
                        .await
                        .unwrap()
                        == pending_response(id)
                );
                maintenance::adopt(&reopened).await.unwrap();
                let replayed = completed(Arc::clone(&reopened), &tenant, id).await;
                check!(
                    replayed.r#async
                        == Some(AsyncQueryResponse {
                            request_id: id.into(),
                            status: AsyncQueryStatus::Success as i32,
                            error_message: String::new(),
                        })
                );
                check!(replayed.flamegraph.unwrap().total == 0);
                check!(
                    record::load(&reopened, &tenant, id)
                        .await
                        .unwrap()
                        .0
                        .adoptions
                        == 1
                );
            } else {
                check!(
                    async_stacktrace_query(Arc::clone(&reopened), tenant.clone(), request(id))
                        .await
                        .unwrap()
                        == expected
                );
                let terminal = record::load(&reopened, &tenant, id).await.unwrap().0;
                maintenance::adopt(&reopened).await.unwrap();
                check!(record::load(&reopened, &tenant, id).await.unwrap().0 == terminal);
            }
            reopened.shutdown_async_queries().await;
        }
    }

    #[tokio::test]
    async fn shutdown_joins_tasks_and_prevents_late_dispatch() {
        let state = Arc::new(QuerierState::empty());
        let stop = state.async_runtime.stop.clone();
        let exited = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&exited);
        check!(state.async_runtime.spawn(async move {
            stop.cancelled().await;
            observed.store(true, Ordering::Release);
        }));
        state.shutdown_async_queries().await;
        check!(exited.load(Ordering::Acquire));
        check!(!state.async_runtime.spawn(async {}));
        let tenant: TenantId = "tenant-a".parse().unwrap();
        check!(
            async_stacktrace_query(state, tenant, request(""))
                .await
                .unwrap_err()
                .code()
                == Code::Unavailable
        );
    }

    struct GatedStore {
        inner: super::super::InMemoryProfileStore,
        entered: std::sync::atomic::AtomicUsize,
        permit: tokio::sync::Semaphore,
    }

    #[async_trait::async_trait]
    impl ProfileStore for GatedStore {
        async fn select(
            &self,
            tenant: &str,
            profile_type: &str,
            matchers: &[krabka_blockstore::LabelMatcher],
            start: i64,
            end: i64,
        ) -> Result<krabka_pprof::ProfileScan, krabka_pprof::ProfileError> {
            self.entered.fetch_add(1, Ordering::AcqRel);
            self.permit.acquire().await.unwrap().forget();
            self.inner
                .select(tenant, profile_type, matchers, start, end)
                .await
        }
        async fn query_stats(
            &self,
            tenant: &str,
            profile_type: &str,
            matchers: &[krabka_blockstore::LabelMatcher],
            start: i64,
            end: i64,
        ) -> Result<krabka_pprof::ProfileQueryStats, krabka_pprof::ProfileError> {
            self.inner
                .query_stats(tenant, profile_type, matchers, start, end)
                .await
        }
        async fn label_names(
            &self,
            tenant: &str,
            matchers: &[krabka_blockstore::LabelMatcher],
            start: i64,
            end: i64,
        ) -> Result<Vec<String>, krabka_pprof::ProfileError> {
            self.inner.label_names(tenant, matchers, start, end).await
        }
        async fn label_values(
            &self,
            tenant: &str,
            name: &str,
            matchers: &[krabka_blockstore::LabelMatcher],
            start: i64,
            end: i64,
        ) -> Result<Vec<String>, krabka_pprof::ProfileError> {
            self.inner
                .label_values(tenant, name, matchers, start, end)
                .await
        }
        async fn profile_types(
            &self,
            tenant: &str,
            start: i64,
            end: i64,
        ) -> Result<Vec<String>, krabka_pprof::ProfileError> {
            self.inner.profile_types(tenant, start, end).await
        }
        async fn series(
            &self,
            tenant: &str,
            matchers: &[krabka_blockstore::LabelMatcher],
            names: &[String],
            start: i64,
            end: i64,
        ) -> Result<Vec<Vec<(String, String)>>, krabka_pprof::ProfileError> {
            self.inner.series(tenant, matchers, names, start, end).await
        }
        async fn stats(
            &self,
            tenant: &str,
            start: i64,
            end: i64,
        ) -> Result<krabka_pprof::ProfileStats, krabka_pprof::ProfileError> {
            self.inner.stats(tenant, start, end).await
        }
    }

    async fn entered(store: &GatedStore, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.entered.load(Ordering::Acquire) < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn shutdown_finishes_the_in_flight_heartbeat_before_relinquishing_ownership() {
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let id = "99999999-9999-4999-8999-999999999999";
        let gate = Arc::new(SuspendedPutStore {
            inner: Arc::new(object_store::memory::InMemory::new()),
            location: record::metadata_path(&tenant, id, 1),
            armed: std::sync::atomic::AtomicBool::new(true),
            fail_put: false,
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let store = Arc::new(GatedStore {
            inner: super::super::InMemoryProfileStore::new(),
            entered: std::sync::atomic::AtomicUsize::new(0),
            permit: tokio::sync::Semaphore::new(0),
        });
        let state = Arc::new(QuerierState::new(Arc::clone(&store)).with_admin_store(gate.clone()));
        state
            .admin_store
            .put(
                &request_path(&tenant, id),
                request("").encode_to_vec().into(),
            )
            .await
            .unwrap();
        let metadata = record::Record {
            version: 1,
            generation: 0,
            tenant: tenant.as_str().into(),
            id: id.into(),
            owner: state.async_runtime.owner.clone(),
            heartbeat_ms: record::now_ms(),
            adoptions: 0,
            status: AsyncQueryStatus::InProgress as i32,
            error: String::new(),
            result_generation: None,
        };
        check!(
            record::write(&state, &metadata, PutMode::Create)
                .await
                .unwrap()
        );
        check!(reserve(&state, &tenant, id).await);
        check!(spawn_query(&state, tenant.clone(), id.into(), request("")));
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        entered(&store, 1).await;
        let joining = tokio::spawn({
            let state = Arc::clone(&state);
            async move { state.shutdown_async_queries().await }
        });
        // Cancellation is observed while the immutable generation put is
        // suspended. Completing it must precede the release generation.
        state.async_runtime.stop.cancelled().await;
        gate.resume.notify_one();
        tokio::time::timeout(Duration::from_secs(5), joining)
            .await
            .unwrap()
            .unwrap();
        let mut expected = metadata;
        expected.generation = 2;
        expected.heartbeat_ms = 1;
        check!(record::load(&state, &tenant, id).await.unwrap().0 == expected);
        check!(state.async_query_slots.lock().await.is_empty());
        check!(store.permit.available_permits() == 0);
    }

    #[tokio::test]
    async fn a_heartbeating_worker_survives_scans_and_shutdown_restarts_the_persisted_query() {
        let directory = tempfile::tempdir().unwrap();
        let admin: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
        );
        let store = Arc::new(GatedStore {
            inner: super::super::InMemoryProfileStore::new(),
            entered: std::sync::atomic::AtomicUsize::new(0),
            permit: tokio::sync::Semaphore::new(0),
        });
        let policy = AsyncQueryPolicy {
            heartbeat_interval: Duration::from_millis(5),
            lease_timeout: Duration::from_secs(1),
            ..Default::default()
        };
        let first = Arc::new(
            QuerierState::new(Arc::clone(&store))
                .with_admin_store(admin.clone())
                .with_async_query_policy(policy.clone())
                .unwrap(),
        );
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let submission = async_stacktrace_query(Arc::clone(&first), tenant.clone(), request(""))
            .await
            .unwrap();
        let id = submission.r#async.unwrap().request_id;
        entered(&store, 1).await;
        let (before, _) = record::load(&first, &tenant, &id).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (current, _) = record::load(&first, &tenant, &id).await.unwrap();
                if current.heartbeat_ms > before.heartbeat_ms {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let reopened: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
        );
        let replacement = Arc::new(
            QuerierState::new(Arc::clone(&store))
                .with_admin_store(reopened)
                .with_async_query_policy(policy)
                .unwrap(),
        );
        maintenance::adopt(&replacement).await.unwrap();
        check!(store.entered.load(Ordering::Acquire) == 1);
        // Even after every existing object exceeds retention, an active
        // worker must retain the original spec for a later owner to replay.
        check!(
            maintenance::cleanup(&first, record::now_ms() + 1_800_001)
                .await
                .unwrap()
                == 0
        );
        check!(
            first
                .admin_store
                .head(&request_path(&tenant, &id))
                .await
                .is_ok()
        );
        first.shutdown_async_queries().await;
        check!(first.async_query_slots.lock().await.is_empty());
        check!(
            record::load(&replacement, &tenant, &id)
                .await
                .unwrap()
                .0
                .heartbeat_ms
                == 1
        );
        maintenance::adopt(&replacement).await.unwrap();
        entered(&store, 2).await;
        store.permit.add_permits(1);
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let response =
                    async_stacktrace_query(Arc::clone(&replacement), tenant.clone(), request(&id))
                        .await
                        .unwrap();
                if response.r#async.as_ref().unwrap().status == AsyncQueryStatus::Success as i32 {
                    break response;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        check!(result.flamegraph.unwrap().total == 0);
        check!(
            record::load(&replacement, &tenant, &id)
                .await
                .unwrap()
                .0
                .adoptions
                == 1
        );
        check!(store.entered.load(Ordering::Acquire) == 2);
        replacement.shutdown_async_queries().await;
    }

    #[tokio::test]
    async fn concurrency_slots_are_per_tenant_and_are_released_on_shutdown() {
        let store = Arc::new(GatedStore {
            inner: super::super::InMemoryProfileStore::new(),
            entered: std::sync::atomic::AtomicUsize::new(0),
            permit: tokio::sync::Semaphore::new(0),
        });
        let state = Arc::new(QuerierState::new_with_limits(
            Arc::clone(&store),
            super::super::Limits {
                max_async_query_concurrency: 1,
                ..Default::default()
            },
        ));
        let a: TenantId = "tenant-a".parse().unwrap();
        let b: TenantId = "tenant-b".parse().unwrap();
        async_stacktrace_query(Arc::clone(&state), a.clone(), request(""))
            .await
            .unwrap();
        entered(&store, 1).await;
        check!(
            async_stacktrace_query(Arc::clone(&state), a, request(""))
                .await
                .unwrap_err()
                .code()
                == Code::ResourceExhausted
        );
        async_stacktrace_query(Arc::clone(&state), b, request(""))
            .await
            .unwrap();
        entered(&store, 2).await;
        state.shutdown_async_queries().await;
        check!(state.async_query_slots.lock().await.is_empty());
    }

    #[tokio::test]
    async fn the_enabled_router_starts_adoption_without_polling_replay() {
        let state = Arc::new(
            QuerierState::empty()
                .with_query_architecture(super::super::PyroscopeQueryArchitecture::V2)
                .with_async_queries_enabled(true)
                .with_async_query_policy(AsyncQueryPolicy {
                    adoption_interval: Duration::from_millis(5),
                    ..Default::default()
                })
                .unwrap(),
        );
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let id = "77777777-7777-4777-8777-777777777777";
        publish_pending(&state, &tenant, id, 1, 0).await;
        let _router = super::super::router(Arc::clone(&state));
        let result = completed(Arc::clone(&state), &tenant, id).await;
        check!(result.r#async.unwrap().status == AsyncQueryStatus::Success as i32);
        check!(record::load(&state, &tenant, id).await.unwrap().0.adoptions == 1);
        state.shutdown_async_queries().await;
        check!(state.async_query_slots.lock().await.is_empty());
    }
    #[tokio::test]
    async fn invalid_latest_metadata_does_not_starve_adoption_or_cleanup_of_other_queries() {
        use futures::TryStreamExt as _;
        for invalid in ["corrupt", "future version", "wrong tenant"] {
            let state = Arc::new(QuerierState::empty());
            let tenant: TenantId = "tenant-a".parse().unwrap();
            let bad = "11111111-1111-4111-8111-111111111111";
            let healthy = "22222222-2222-4222-8222-222222222222";
            publish_pending(&state, &tenant, bad, 1, 0).await;
            let (mut invalid_record, _) = record::load(&state, &tenant, bad).await.unwrap();
            invalid_record.generation = 1;
            if invalid == "future version" {
                invalid_record.version = 2;
            }
            if invalid == "wrong tenant" {
                invalid_record.tenant = "tenant-b".into();
            }
            let invalid_bytes = if invalid == "corrupt" {
                b"{".to_vec()
            } else {
                serde_json::to_vec(&invalid_record).unwrap()
            };
            let invalid_path = record::metadata_path(&tenant, bad, 1);
            state
                .admin_store
                .put(&invalid_path, invalid_bytes.clone().into())
                .await
                .unwrap();
            let prefix = Path::from(format!("profiles-admin/{tenant}/async/{bad}"));
            let original_objects = state
                .admin_store
                .list(Some(&prefix))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut original = std::collections::BTreeMap::new();
            for object in original_objects {
                original.insert(
                    object.location.clone(),
                    state
                        .admin_store
                        .get(&object.location)
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap(),
                );
            }
            publish_pending(&state, &tenant, healthy, 1, 0).await;
            // The older valid generation is encountered first. Loading the
            // newer invalid generation must reject that query, not the scan.
            maintenance::adopt(&state).await.unwrap();
            let response = completed(Arc::clone(&state), &tenant, healthy).await;
            let (current, _) = record::load(&state, &tenant, healthy).await.unwrap();
            check!(
                (
                    response.r#async.as_ref().unwrap().status,
                    response.flamegraph.as_ref().unwrap().total,
                    current.adoptions,
                    current.owner.as_str()
                ) == (
                    AsyncQueryStatus::Success as i32,
                    0,
                    1,
                    state.async_runtime.owner.as_str()
                )
            );
            let error = record::load(&state, &tenant, bad).await.unwrap_err();
            check!(
                error.code()
                    == if invalid == "wrong tenant" {
                        Code::NotFound
                    } else {
                        Code::Internal
                    }
            );
            let expiry = current.heartbeat_ms + 1_800_001;
            check!(maintenance::cleanup(&state, expiry).await.unwrap() > 0);
            let healthy_prefix = Path::from(format!("profiles-admin/{tenant}/async/{healthy}"));
            check!(
                state
                    .admin_store
                    .list(Some(&healthy_prefix))
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|object| object.location)
                    .collect::<Vec<_>>()
                    == vec![record::expiry_path(&tenant, healthy)]
            );
            let mut remaining = std::collections::BTreeMap::new();
            for object in state
                .admin_store
                .list(Some(&prefix))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
            {
                remaining.insert(
                    object.location.clone(),
                    state
                        .admin_store
                        .get(&object.location)
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap(),
                );
            }
            // Every byte of the rejected group remains unchanged; no adoption,
            // failure publication, deletion or fallback to the older record.
            check!(remaining == original);
            state.shutdown_async_queries().await;
        }
    }

    #[tokio::test]
    async fn cleanup_expires_only_old_metadata_free_objects_and_retains_fresh_or_recoverable_files()
    {
        use futures::TryStreamExt as _;
        let directory = tempfile::tempdir().unwrap();
        let admin: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap(),
        );
        let state = QuerierState::empty()
            .with_admin_store(Arc::clone(&admin))
            .with_async_query_policy(AsyncQueryPolicy {
                retention: Duration::from_secs(10),
                ..Default::default()
            })
            .unwrap();
        let tenant: TenantId = "tenant-a".parse().unwrap();
        let old = "33333333-3333-4333-8333-333333333333";
        let fresh = "44444444-4444-4444-8444-444444444444";
        let mixed = "55555555-5555-4555-8555-555555555555";
        let malformed = "66666666-6666-4666-8666-666666666666";
        let active = "77777777-7777-4777-8777-777777777777";
        let malformed_metadata = Path::from(format!(
            "profiles-admin/{tenant}/async/{malformed}/metadata/unversioned.json"
        ));
        let unrelated = Path::from("profiles-admin/tenant-a/settings.json");
        let fixtures = [
            (request_path(&tenant, old), true),
            (completed_path(&tenant, old, 1), true),
            (request_path(&tenant, fresh), false),
            (completed_path(&tenant, fresh, 1), false),
            (request_path(&tenant, mixed), false),
            (completed_path(&tenant, mixed, 1), true),
            (request_path(&tenant, malformed), true),
            (malformed_metadata.clone(), true),
            (unrelated.clone(), true),
        ];
        for (path, is_old) in &fixtures {
            admin.put(path, b"fixture".to_vec().into()).await.unwrap();
            // Actual filesystem metadata is the independent storage clock.
            // Fresh objects sit exactly at the cutoff, proving strict expiry
            // without depending on elapsed wall time or sleeping.
            std::fs::File::open(directory.path().join(path.as_ref()))
                .unwrap()
                .set_modified(
                    std::time::UNIX_EPOCH + Duration::from_secs(if *is_old { 1 } else { 990 }),
                )
                .unwrap();
        }
        publish_pending(&state, &tenant, active, 1, 0).await;
        for path in [
            request_path(&tenant, active),
            record::metadata_path(&tenant, active, 0),
        ] {
            std::fs::File::open(directory.path().join(path.as_ref()))
                .unwrap()
                .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(1))
                .unwrap();
        }
        check!(
            admin
                .head(&request_path(&tenant, old))
                .await
                .unwrap()
                .last_modified
                .timestamp_millis()
                == 1000
        );
        check!(maintenance::cleanup(&state, 1_000_000).await.unwrap() == 3);
        let mut remaining = admin
            .list(Some(&Path::from("profiles-admin")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.location)
            .collect::<Vec<_>>();
        remaining.sort();
        let mut expected = vec![
            record::expiry_path(&tenant, old),
            request_path(&tenant, fresh),
            completed_path(&tenant, fresh, 1),
            request_path(&tenant, mixed),
            request_path(&tenant, malformed),
            malformed_metadata,
            unrelated,
            request_path(&tenant, active),
            record::metadata_path(&tenant, active, 0),
        ];
        expected.sort();
        check!(remaining == expected);
        check!(record::load(&state, &tenant, old).await.unwrap_err().code() == Code::NotFound);
        let (mut delayed_initial, _) = record::load(&state, &tenant, active).await.unwrap();
        delayed_initial.id = old.into();
        check!(
            !record::write(&state, &delayed_initial, PutMode::Create)
                .await
                .unwrap()
        );
        // Mixed-age groups retain the ability to recover their fresh request.
        check!(!record::is_expired(&state, &tenant, mixed).await.unwrap());
        check!(
            record::load(&state, &tenant, active)
                .await
                .unwrap()
                .0
                .status
                == AsyncQueryStatus::InProgress as i32
        );
    }
}
