use futures::TryStreamExt as _;
use object_store::{ObjectStoreExt, PutMode, path::Path};
use prost::Message;

use super::{
    Arc, AsyncQueryStatus, ConnectError, ProfileStore, QuerierState, SelectMergeStacktracesRequest,
    TenantId, record, release, request_path, reserve, spawn_query, storage_error,
};

pub(super) fn expired<S: ProfileStore>(
    state: &QuerierState<S>,
    record: &record::Record,
    now: i64,
) -> bool {
    record.status == AsyncQueryStatus::InProgress as i32
        && record.heartbeat_ms > 0
        && i128::from(now).saturating_sub(i128::from(record.heartbeat_ms))
            > i128::try_from(state.async_runtime.policy.lease_timeout.as_millis())
                .unwrap_or(i128::MAX)
}

pub(super) async fn fail_record<S: ProfileStore>(
    state: &QuerierState<S>,
    record: record::Record,
    error: String,
) -> Result<bool, ConnectError> {
    let tenant = TenantId::new(&record.tenant)
        .map_err(|e| ConnectError::new(super::Code::Internal, e.to_string()))?;
    let (mut current, version) = record::load(state, &tenant, &record.id).await?;
    if current.owner != record.owner
        || current.heartbeat_ms != record.heartbeat_ms
        || !expired(state, &current, record::now_ms())
    {
        return Ok(false);
    }
    current.status = AsyncQueryStatus::Failure as i32;
    current.error = error;
    current.heartbeat_ms = record::now_ms();
    record::write(state, &current, PutMode::Update(version)).await
}

pub(super) async fn adopt<S: ProfileStore + 'static>(
    state: &Arc<QuerierState<S>>,
) -> Result<(), ConnectError> {
    let prefix = Path::from("profiles-admin");
    let mut objects = state.admin_store.list(Some(&prefix));
    let mut visited = std::collections::BTreeSet::new();
    while let Some(object) = objects.try_next().await.map_err(|e| storage_error(&e))? {
        if state.async_runtime.stop.is_cancelled() {
            break;
        }
        let name = object.location.as_ref();
        if !name.contains("/async/")
            || !name.contains("/metadata/")
            || name.strip_suffix(".json").is_none()
        {
            continue;
        }
        let location = object.location;
        let object = match state.admin_store.get(&location).await {
            Ok(object) => object,
            Err(error) => {
                tracing::warn!(%error, %location, "failed to read async metadata; skipping record");
                continue;
            }
        };
        let bytes = match object.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(%error, %location, "failed to read async metadata body; skipping record");
                continue;
            }
        };
        let metadata: record::Record = match serde_json::from_slice(&bytes) {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::warn!(%error, %location, "invalid async metadata; skipping record");
                continue;
            }
        };
        if metadata.version != 1 {
            tracing::warn!(%location, "unsupported async metadata version; skipping record");
            continue;
        }
        let tenant = match TenantId::new(&metadata.tenant) {
            Ok(tenant) => tenant,
            Err(error) => {
                tracing::warn!(%error, %location, "invalid async tenant; skipping record");
                continue;
            }
        };
        let id = metadata.id.clone();
        if location != record::metadata_path(&tenant, &id, metadata.generation)
            || !visited.insert((tenant.as_str().to_string(), id.clone()))
        {
            continue;
        }
        let (metadata, _) = match record::load(state, &tenant, &id).await {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(?error, %tenant, %id, "failed to validate latest async metadata; skipping query");
                continue;
            }
        };
        if !expired(state, &metadata, record::now_ms()) {
            continue;
        }
        let spec = match state.admin_store.get(&request_path(&tenant, &id)).await {
            Ok(object) => {
                let bytes = match object.bytes().await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::warn!(%error, %tenant, %id, "failed to read async spec; skipping query");
                        continue;
                    }
                };
                if let Ok(spec) = SelectMergeStacktracesRequest::decode(bytes) {
                    spec
                } else {
                    if let Err(error) = fail_record(
                        state,
                        metadata,
                        "query orphaned: request spec is corrupt".into(),
                    )
                    .await
                    {
                        tracing::warn!(?error, %tenant, %id, "failed to mark corrupt async spec; skipping query");
                    }
                    continue;
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                if let Err(error) = fail_record(state, metadata, "query orphaned: owner heartbeat expired and no request spec is available for adoption".into()).await {
                    tracing::warn!(?error, %tenant, %id, "failed to mark missing async spec; skipping query");
                }
                continue;
            }
            Err(error) => {
                tracing::warn!(?error, "failed to read async spec before adoption");
                continue;
            }
        };
        if !reserve(state, &tenant, &id).await {
            continue;
        }
        let claimed = async {
            let (mut current, version) = record::load(state, &tenant, &id).await?;
            if !expired(state, &current, record::now_ms()) {
                return Ok(false);
            }
            if current.adoptions >= state.async_runtime.policy.maximum_adoptions {
                fail_record(
                    state,
                    current,
                    "query exceeded the maximum number of adoption attempts".into(),
                )
                .await?;
                return Ok(false);
            }
            // A conditional update fences concurrent scanners and old heartbeats.
            current.owner.clone_from(&state.async_runtime.owner);
            current.heartbeat_ms = record::now_ms();
            current.adoptions += 1;
            record::write(state, &current, PutMode::Update(version)).await
        }
        .await;
        match claimed {
            Ok(true) => {
                if !spawn_query(state, tenant.clone(), id.clone(), spec) {
                    if let Err(error) = record::renew(state, &tenant, &id, true).await {
                        tracing::warn!(?error, %tenant, %id, "failed to relinquish unstarted async query");
                    }
                    release(state, &tenant, &id).await;
                }
            }
            Ok(false) => release(state, &tenant, &id).await,
            Err(error) => {
                release(state, &tenant, &id).await;
                tracing::warn!(?error, "failed to claim async query");
            }
        }
    }
    Ok(())
}

pub(super) async fn cleanup<S: ProfileStore>(
    state: &QuerierState<S>,
    now_ms: i64,
) -> Result<usize, ConnectError> {
    let cutoff = i128::from(now_ms)
        - i128::try_from(state.async_runtime.policy.retention.as_millis()).unwrap_or(i128::MAX);
    let prefix = Path::from("profiles-admin");
    let mut objects = state.admin_store.list(Some(&prefix));
    let mut groups =
        std::collections::BTreeMap::<(String, String), Vec<object_store::ObjectMeta>>::new();
    while let Some(object) = objects
        .try_next()
        .await
        .map_err(|error| storage_error(&error))?
    {
        let segments = object.location.as_ref().split('/').collect::<Vec<_>>();
        if segments.len() >= 5
            && segments[0] == "profiles-admin"
            && segments[2] == "async"
            && uuid::Uuid::parse_str(segments[3]).is_ok()
        {
            groups
                .entry((segments[1].to_string(), segments[3].to_string()))
                .or_default()
                .push(object);
        }
    }
    let mut deleted = 0;
    'groups: for ((tenant, id), mut objects) in groups {
        if state.async_runtime.stop.is_cancelled() {
            break;
        }
        let tenant = match TenantId::new(&tenant) {
            Ok(tenant) => tenant,
            Err(error) => {
                tracing::warn!(%error, %id, "invalid async cleanup tenant; skipping group");
                continue;
            }
        };
        let prefix = record::metadata_prefix(&tenant, &id);
        let has_metadata = objects
            .iter()
            .any(|object| object.location.prefix_matches(&prefix));
        let sealed = match record::is_expired(state, &tenant, &id).await {
            Ok(sealed) => sealed,
            Err(error) => {
                tracing::warn!(?error, %tenant, %id, "invalid async expiry seal; skipping group");
                continue;
            }
        };
        let orphaned = if sealed {
            // Late uploads cannot revive the query. Purge them even when their
            // modification times are fresh, retaining only the permanent seal.
            false
        } else {
            match record::load(state, &tenant, &id).await {
                Ok((metadata, _)) => {
                    // Recovery files live as long as a current query. Terminal
                    // retention is independent of individual recovery-file ages.
                    if !matches!(metadata.status, status if status == AsyncQueryStatus::Success as i32 || status == AsyncQueryStatus::Failure as i32)
                        || i128::from(metadata.heartbeat_ms) >= cutoff
                    {
                        continue;
                    }
                    false
                }
                Err(error) if error.code() == super::Code::NotFound && !has_metadata => {
                    // NotFound can also mean an identity mismatch or a concurrent
                    // metadata disappearance. Confirm actual absence; never turn
                    // malformed/unsupported metadata into permission to delete.
                    match state.admin_store.list(Some(&prefix)).try_next().await {
                        Ok(None) => true,
                        Ok(Some(_)) => continue,
                        Err(error) => {
                            tracing::warn!(%error, %tenant, %id, "failed to confirm absent async metadata; skipping group");
                            continue;
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, %tenant, %id, "failed to validate async cleanup metadata; skipping group");
                    continue;
                }
            }
        };
        // Seal a terminal query before deleting any generation. Entirely old
        // orphan groups need the same fence against a delayed initial create;
        // mixed-age orphan groups still expire objects individually.
        if !sealed
            && (!orphaned
                || objects
                    .iter()
                    .all(|object| i128::from(object.last_modified.timestamp_millis()) < cutoff))
            && let Err(error) = record::seal_expiry(state, &tenant, &id, now_ms).await
        {
            tracing::warn!(?error, %tenant, %id, "failed to seal async expiry; skipping group");
            continue;
        }
        // Delete older metadata before the current terminal generation, and
        // stop this group on a failure. Otherwise an older in-progress record
        // could become visible after a partially failed cleanup.
        objects.sort_by(|left, right| {
            (left.location.prefix_matches(&prefix), &left.location)
                .cmp(&(right.location.prefix_matches(&prefix), &right.location))
        });
        for object in objects {
            if object.location == record::expiry_path(&tenant, &id) {
                continue;
            }
            if orphaned && i128::from(object.last_modified.timestamp_millis()) >= cutoff {
                continue;
            }
            match state.admin_store.delete(&object.location).await {
                Ok(()) => deleted += 1,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => {
                    tracing::warn!(%error, %tenant, %id, path = %object.location, "failed to expire async object; skipping group");
                    continue 'groups;
                }
            }
        }
    }
    Ok(deleted)
}
