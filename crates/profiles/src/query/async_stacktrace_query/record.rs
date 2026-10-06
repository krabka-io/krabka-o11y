use std::time::{SystemTime, UNIX_EPOCH};

use futures::TryStreamExt as _;
use object_store::{ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path};
use serde::{Deserialize, Serialize};

use super::{AsyncQueryStatus, ConnectError, ProfileStore, QuerierState, TenantId, storage_error};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct Record {
    pub version: u32,
    pub generation: u64,
    pub tenant: String,
    pub id: String,
    pub owner: String,
    pub heartbeat_ms: i64,
    pub adoptions: u32,
    pub status: i32,
    pub error: String,
    pub result_generation: Option<u64>,
}

// Payloads expire, but the small immutable seal remains for the lifetime of
// this query ID. Removing it would reopen generation keys to suspended owners.
// ponytail: one seal per expired query; compact only with an equivalent durable
// fence that still rejects arbitrarily delayed owners.
#[derive(Debug, Serialize, Deserialize)]
struct ExpirySeal {
    version: u32,
    tenant: String,
    id: String,
    expired_ms: i64,
}

pub(super) fn expiry_path(tenant: &TenantId, id: &str) -> Path {
    Path::from(format!("profiles-admin/{tenant}/async/{id}/expired.json"))
}

pub(super) async fn is_expired<S: ProfileStore>(
    state: &QuerierState<S>,
    tenant: &TenantId,
    id: &str,
) -> Result<bool, ConnectError> {
    let object = match state.admin_store.get(&expiry_path(tenant, id)).await {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => return Ok(false),
        Err(error) => return Err(storage_error(&error)),
    };
    let seal: ExpirySeal = serde_json::from_slice(
        &object
            .bytes()
            .await
            .map_err(|error| storage_error(&error))?,
    )
    .map_err(|error| ConnectError::new(super::Code::Internal, error.to_string()))?;
    if seal.version != 1 || seal.tenant != tenant.as_str() || seal.id != id {
        return Err(ConnectError::new(
            super::Code::Internal,
            "unsupported async expiry seal version or identity",
        ));
    }
    Ok(true)
}

pub(super) async fn seal_expiry<S: ProfileStore>(
    state: &QuerierState<S>,
    tenant: &TenantId,
    id: &str,
    expired_ms: i64,
) -> Result<(), ConnectError> {
    // Validate an existing seal before any deletion, including future versions.
    if is_expired(state, tenant, id).await? {
        return Ok(());
    }
    let bytes = serde_json::to_vec(&ExpirySeal {
        version: 1,
        tenant: tenant.as_str().into(),
        id: id.into(),
        expired_ms,
    })
    .map_err(|error| ConnectError::new(super::Code::Internal, error.to_string()))?;
    match state
        .admin_store
        .put_opts(
            &expiry_path(tenant, id),
            bytes.into(),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(
            object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. },
        ) => {
            is_expired(state, tenant, id).await?;
            Ok(())
        }
        Err(error) => Err(storage_error(&error)),
    }
}

fn not_found() -> ConnectError {
    ConnectError::new(super::Code::NotFound, "async query not found")
}

pub(super) fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

pub(super) fn metadata_prefix(tenant: &TenantId, id: &str) -> Path {
    Path::from(format!("profiles-admin/{tenant}/async/{id}/metadata"))
}

pub(super) fn metadata_path(tenant: &TenantId, id: &str, generation: u64) -> Path {
    Path::from(format!(
        "{}/{generation:020}.json",
        metadata_prefix(tenant, id)
    ))
}

pub(super) async fn load<S: ProfileStore>(
    state: &QuerierState<S>,
    tenant: &TenantId,
    id: &str,
) -> Result<(Record, UpdateVersion), ConnectError> {
    if is_expired(state, tenant, id).await? {
        return Err(not_found());
    }
    let prefix = metadata_prefix(tenant, id);
    let objects = state
        .admin_store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| storage_error(&error))?;
    let (generation, location) = objects
        .into_iter()
        .filter_map(|object| {
            let generation = object
                .location
                .filename()?
                .strip_suffix(".json")?
                .parse::<u64>()
                .ok()?;
            (object.location == metadata_path(tenant, id, generation))
                .then_some((generation, object.location))
        })
        .max_by_key(|(generation, _)| *generation)
        .ok_or_else(|| ConnectError::new(super::Code::NotFound, "async query not found"))?;
    let object = state
        .admin_store
        .get(&location)
        .await
        .map_err(|error| storage_error(&error))?;
    let record: Record = serde_json::from_slice(
        &object
            .bytes()
            .await
            .map_err(|error| storage_error(&error))?,
    )
    .map_err(|error| ConnectError::new(super::Code::Internal, error.to_string()))?;
    if record.version != 1 || record.generation != generation {
        return Err(ConnectError::new(
            super::Code::Internal,
            "unsupported async metadata version or generation",
        ));
    }
    if record.tenant != tenant.as_str() || record.id != id {
        return Err(ConnectError::new(
            super::Code::NotFound,
            "async query not found",
        ));
    }
    if is_expired(state, tenant, id).await? {
        return Err(not_found());
    }
    Ok((
        record,
        UpdateVersion {
            e_tag: None,
            version: Some(generation.to_string()),
        },
    ))
}

pub(super) async fn write<S: ProfileStore>(
    state: &QuerierState<S>,
    record: &Record,
    mode: PutMode,
) -> Result<bool, ConnectError> {
    let tenant = TenantId::new(&record.tenant)
        .map_err(|error| ConnectError::new(super::Code::Internal, error.to_string()))?;
    if is_expired(state, &tenant, &record.id).await? {
        return Ok(false);
    }
    let mut next = record.clone();
    match mode {
        PutMode::Create if record.generation == 0 => {}
        PutMode::Update(version)
            if version
                .version
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                == Some(record.generation) =>
        {
            next.generation = record.generation.checked_add(1).ok_or_else(|| {
                ConnectError::new(super::Code::Internal, "async metadata generation overflow")
            })?;
        }
        _ => {
            return Err(ConnectError::new(
                super::Code::Internal,
                "async metadata requires conditional publication",
            ));
        }
    }
    let bytes = serde_json::to_vec(&next)
        .map_err(|error| ConnectError::new(super::Code::Internal, error.to_string()))?;
    // As with durable index manifests, writers observing generation N contend
    // for one immutable N+1 key. Conditional create is the ownership CAS; it
    // works on local files as well as cloud stores with strong listing.
    match state
        .admin_store
        .put_opts(
            &metadata_path(&tenant, &next.id, next.generation),
            bytes.into(),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        // A put can resume after expiry removed the old generation keys.
        // The seal is the permanent fence; even that late upload must fail
        // publication. Readers also consult it, and cleanup removes leftovers.
        Ok(_) => Ok(!is_expired(state, &tenant, &next.id).await?),
        Err(
            object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. },
        ) => Ok(false),
        Err(error) => Err(storage_error(&error)),
    }
}

pub(super) async fn renew<S: ProfileStore>(
    state: &QuerierState<S>,
    tenant: &TenantId,
    id: &str,
    release: bool,
) -> Result<bool, ConnectError> {
    if is_expired(state, tenant, id).await? {
        return Ok(false);
    }
    let (mut record, version) = load(state, tenant, id).await?;
    if record.owner != state.async_runtime.owner
        || record.status != AsyncQueryStatus::InProgress as i32
    {
        return Ok(false);
    }
    record.heartbeat_ms = if release { 1 } else { now_ms() };
    write(state, &record, PutMode::Update(version)).await
}

#[cfg(test)]
pub(super) async fn terminal<S: ProfileStore>(
    state: &QuerierState<S>,
    tenant: &TenantId,
    id: &str,
    status: i32,
    error: String,
) -> Result<(), ConnectError> {
    let (mut record, version) = load(state, tenant, id).await?;
    if record.owner != state.async_runtime.owner
        || record.status != AsyncQueryStatus::InProgress as i32
    {
        return Ok(());
    }
    record.status = status;
    record.error = error;
    record.heartbeat_ms = now_ms();
    write(state, &record, PutMode::Update(version)).await?;
    Ok(())
}
