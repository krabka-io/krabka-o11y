#![allow(clippy::result_large_err)]

use std::{
    collections::BTreeSet,
    io::Read as _,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Extension,
    extract::{Path as AxumPath, Request},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse as _, Response},
};
use base64::Engine as _;
use connectrpc_axum::message::{Code, ConnectError, ConnectRequest, ConnectResponse};
use flate2::read::GzDecoder;
use futures::TryStreamExt as _;
use krabka_blockstore::{LABEL_PROFILE_TYPE, MatchOp, TenantId};
use krabka_observability::server_security::{Principal, authorize_tenant};
use krabka_pprof::{Frame, PprofProfile, ProfileStore, Tree, diff_trees, parse_label_selector};
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{
    QuerierState, pb, tenant_connect_error, tenant_denied_connect_error, tenant_from_headers,
};

const ADMIN_PREFIX: &str = "profiles-admin";
const ADHOC_MAX_BYTES: usize = 100 * 1024 * 1024;
const DEBUGINFO_MAX_BYTES: usize = 1024 * 1024 * 1024;
const UPLOAD_STALE_SECS: i64 = 7 * 60;

const FIRST_SEEN: &str = "First time we see this Build ID, therefore please upload!";
const UPLOAD_STALE: &str =
    "A previous upload was started but not finished and is now stale, so it can be retried.";
const UPLOAD_IN_PROGRESS: &str =
    "A previous upload is still in-progress and not stale yet (only stale uploads can be retried).";
const ALREADY_EXISTS: &str =
    "Debuginfo already exists and is not marked as invalid, therefore no new upload is needed.";
const EMPTY_BUILD_ID: &str = "Empty GNU build ID, therefore no upload is needed.";

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn admin_error(code: Code, message: impl Into<String>) -> ConnectError {
    ConnectError::new(code, message.into())
}

fn tenant<S: ProfileStore>(
    state: &QuerierState<S>,
    principal: &Principal,
    headers: &HeaderMap,
) -> Result<TenantId, ConnectError> {
    let tenant = tenant_from_headers(headers, &state.tenant_policy)
        .map_err(|error| tenant_connect_error(&error))?;
    authorize_tenant(principal, &tenant).map_err(|denied| tenant_denied_connect_error(&denied))?;
    Ok(tenant)
}

async fn read_message<M: Message + Default>(
    store: &dyn ObjectStore,
    key: &str,
) -> Result<Option<M>, ConnectError> {
    let object = match store.get(&Path::from(key)).await {
        Ok(object) => object,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(admin_error(Code::Internal, error.to_string())),
    };
    let bytes = object
        .bytes()
        .await
        .map_err(|error| admin_error(Code::Internal, error.to_string()))?;
    M::decode(bytes).map(Some).map_err(|error| {
        admin_error(
            Code::Internal,
            format!("persisted tenant state is malformed: {error}"),
        )
    })
}

async fn write_message<M: Message>(
    store: &dyn ObjectStore,
    key: &str,
    value: &M,
) -> Result<(), ConnectError> {
    store
        .put(&Path::from(key), value.encode_to_vec().into())
        .await
        .map(|_| ())
        .map_err(|error| admin_error(Code::Internal, error.to_string()))
}

async fn delete_object(store: &dyn ObjectStore, key: &str) -> Result<(), ConnectError> {
    match store.delete(&Path::from(key)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(admin_error(Code::Internal, error.to_string())),
    }
}

fn settings_key(tenant: &TenantId) -> String {
    format!("{ADMIN_PREFIX}/{tenant}/settings.pb")
}

pub(crate) async fn get_settings_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    _req: ConnectRequest<pb::settings::v1::GetSettingsRequest>,
) -> Result<ConnectResponse<pb::settings::v1::GetSettingsResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    Ok(ConnectResponse::new(
        read_message(state.admin_store.as_ref(), &settings_key(&tenant))
            .await?
            .unwrap_or_default(),
    ))
}

pub(crate) async fn set_settings_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::settings::v1::SetSettingsRequest>,
) -> Result<ConnectResponse<pb::settings::v1::SetSettingsResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    let mut setting = req
        .0
        .setting
        .ok_or_else(|| admin_error(Code::InvalidArgument, "no setting values provided"))?;
    if setting.name.is_empty() {
        return Err(admin_error(
            Code::InvalidArgument,
            "no setting name provided",
        ));
    }
    if setting.modified_at <= 0 {
        setting.modified_at = now_millis();
    }
    let key = settings_key(&tenant);
    let mut stored: pb::settings::v1::GetSettingsResponse =
        read_message(state.admin_store.as_ref(), &key)
            .await?
            .unwrap_or_default();
    if let Some(existing) = stored
        .settings
        .iter_mut()
        .find(|existing| existing.name == setting.name)
    {
        if setting.modified_at < existing.modified_at {
            return Err(admin_error(
                Code::AlreadyExists,
                "setting is older than stored value",
            ));
        }
        *existing = setting.clone();
    } else {
        stored.settings.push(setting.clone());
    }
    stored
        .settings
        .sort_by(|left, right| left.name.cmp(&right.name));
    write_message(state.admin_store.as_ref(), &key, &stored).await?;
    Ok(ConnectResponse::new(
        pb::settings::v1::SetSettingsResponse {
            setting: Some(setting),
        },
    ))
}

pub(crate) async fn delete_settings_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::settings::v1::DeleteSettingsRequest>,
) -> Result<ConnectResponse<pb::settings::v1::DeleteSettingsResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    if req.0.name.is_empty() {
        return Err(admin_error(
            Code::InvalidArgument,
            "no setting name provided",
        ));
    }
    let key = settings_key(&tenant);
    let mut stored: pb::settings::v1::GetSettingsResponse =
        read_message(state.admin_store.as_ref(), &key)
            .await?
            .unwrap_or_default();
    stored.settings.retain(|setting| setting.name != req.0.name);
    if stored.settings.is_empty() {
        delete_object(state.admin_store.as_ref(), &key).await?;
    } else {
        write_message(state.admin_store.as_ref(), &key, &stored).await?;
    }
    Ok(ConnectResponse::new(
        pb::settings::v1::DeleteSettingsResponse {},
    ))
}

fn rules_key(tenant: &TenantId) -> String {
    format!("{ADMIN_PREFIX}/{tenant}/recording-rules.pb")
}

async fn rules(
    store: &dyn ObjectStore,
    tenant: &TenantId,
) -> Result<pb::settings::v1::ListRecordingRulesResponse, ConnectError> {
    Ok(read_message(store, &rules_key(tenant))
        .await?
        .unwrap_or_default())
}

pub(crate) async fn get_recording_rule_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::settings::v1::GetRecordingRuleRequest>,
) -> Result<ConnectResponse<pb::settings::v1::GetRecordingRuleResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    let rule = rules(state.admin_store.as_ref(), &tenant)
        .await?
        .rules
        .into_iter()
        .find(|rule| rule.id == req.0.id)
        .ok_or_else(|| {
            admin_error(
                Code::NotFound,
                format!("no rule with id='{}' found", req.0.id),
            )
        })?;
    Ok(ConnectResponse::new(
        pb::settings::v1::GetRecordingRuleResponse { rule: Some(rule) },
    ))
}

pub(crate) async fn list_recording_rules_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    _req: ConnectRequest<pb::settings::v1::ListRecordingRulesRequest>,
) -> Result<ConnectResponse<pb::settings::v1::ListRecordingRulesResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    Ok(ConnectResponse::new(
        rules(state.admin_store.as_ref(), &tenant).await?,
    ))
}

pub(crate) async fn upsert_recording_rule_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::settings::v1::UpsertRecordingRuleRequest>,
) -> Result<ConnectResponse<pb::settings::v1::UpsertRecordingRuleResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    let req = req.0;
    validate_metric_name(&req.metric_name)?;
    let profile_type = recording_profile_type(&req.matchers)?;
    if req.group_by.iter().any(|name| !valid_label_name(name))
        || req
            .external_labels
            .iter()
            .any(|label| !valid_label_name(&label.name))
    {
        return Err(admin_error(
            Code::InvalidArgument,
            "recording rule contains an invalid label name",
        ));
    }
    if req.generation < 0 {
        return Err(admin_error(
            Code::InvalidArgument,
            "generation must be positive",
        ));
    }
    let id = if req.id.is_empty() {
        generated_letters()
    } else {
        if !req.id.bytes().all(|byte| byte.is_ascii_alphabetic()) {
            return Err(admin_error(
                Code::InvalidArgument,
                "recording rule id must contain only letters",
            ));
        }
        req.id
    };
    let key = rules_key(&tenant);
    let mut stored = rules(state.admin_store.as_ref(), &tenant).await?;
    let position = stored.rules.iter().position(|rule| rule.id == id);
    let generation = if let Some(position) = position {
        if req.generation != stored.rules[position].generation {
            return Err(admin_error(
                Code::AlreadyExists,
                "conflicting update, please try again",
            ));
        }
        req.generation.saturating_add(1)
    } else {
        1
    };
    let rule = pb::settings::v1::RecordingRule {
        id,
        metric_name: req.metric_name,
        profile_type,
        matchers: req.matchers,
        group_by: req.group_by,
        external_labels: req.external_labels,
        generation,
        stacktrace_filter: req.stacktrace_filter,
        provisioned: false,
    };
    if let Some(position) = position {
        stored.rules[position] = rule.clone();
    } else {
        stored.rules.push(rule.clone());
    }
    stored.rules.sort_by(|left, right| left.id.cmp(&right.id));
    write_message(state.admin_store.as_ref(), &key, &stored).await?;
    Ok(ConnectResponse::new(
        pb::settings::v1::UpsertRecordingRuleResponse { rule: Some(rule) },
    ))
}

pub(crate) async fn delete_recording_rule_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::settings::v1::DeleteRecordingRuleRequest>,
) -> Result<ConnectResponse<pb::settings::v1::DeleteRecordingRuleResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    let key = rules_key(&tenant);
    let mut stored = rules(state.admin_store.as_ref(), &tenant).await?;
    let before = stored.rules.len();
    stored.rules.retain(|rule| rule.id != req.0.id);
    if before == stored.rules.len() {
        return Err(admin_error(
            Code::NotFound,
            format!("no rule with ID='{}' found", req.0.id),
        ));
    }
    if stored.rules.is_empty() {
        delete_object(state.admin_store.as_ref(), &key).await?;
    } else {
        write_message(state.admin_store.as_ref(), &key, &stored).await?;
    }
    Ok(ConnectResponse::new(
        pb::settings::v1::DeleteRecordingRuleResponse {},
    ))
}

fn validate_metric_name(name: &str) -> Result<(), ConnectError> {
    if !name.starts_with("profiles_recorded_") {
        return Err(admin_error(
            Code::InvalidArgument,
            "metric_name must start with profiles_recorded_",
        ));
    }
    let mut bytes = name.bytes();
    let valid_first = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'_' | b':'));
    if !valid_first
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':'))
    {
        return Err(admin_error(Code::InvalidArgument, "metric_name is invalid"));
    }
    Ok(())
}

fn valid_label_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn recording_profile_type(matchers: &[String]) -> Result<String, ConnectError> {
    let mut profile_types = Vec::new();
    for selector in matchers {
        let parsed = parse_label_selector(selector)
            .map_err(|error| admin_error(Code::InvalidArgument, error.to_string()))?;
        profile_types.extend(parsed.into_iter().filter_map(|matcher| {
            (matcher.name == LABEL_PROFILE_TYPE && matcher.op == MatchOp::Eq)
                .then_some(matcher.value)
        }));
    }
    if profile_types.len() != 1 {
        return Err(admin_error(
            Code::InvalidArgument,
            "matchers must contain one __profile_type__ equality matcher",
        ));
    }
    krabka_pprof::ProfileType::parse(&profile_types[0])
        .map_err(|error| admin_error(Code::InvalidArgument, error.to_string()))?;
    Ok(profile_types.remove(0))
}

pub(crate) async fn feature_flags_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    _req: ConnectRequest<pb::capabilities::v1::GetFeatureFlagsRequest>,
) -> Result<ConnectResponse<pb::capabilities::v1::GetFeatureFlagsResponse>, ConnectError> {
    tenant(&state, &principal, &headers)?;
    let flag = |name: &str, description: &str| pb::capabilities::v1::FeatureFlag {
        name: name.to_string(),
        enabled: false,
        description: Some(description.to_string()),
        documentation_url: None,
    };
    Ok(ConnectResponse::new(
        pb::capabilities::v1::GetFeatureFlagsResponse {
            feature_flags: vec![
                flag(
                    "pyroscopeRuler",
                    "Profiling recording-rule evaluation is not enabled.",
                ),
                flag(
                    "pyroscopeRulerFunctions",
                    "Function recording rules are not enabled.",
                ),
                flag("utf8LabelNames", "UTF-8 label names are not enabled."),
                flag("v2StorageLayer", "Pyroscope v2 storage is not enabled."),
            ],
        },
    ))
}

#[derive(Deserialize, Serialize)]
struct AdHocProfile {
    name: String,
    profile: String,
    uploaded_at: i64,
}

fn adhoc_key(tenant: &TenantId, id: &str) -> String {
    format!("{ADMIN_PREFIX}/{tenant}/adhoc/{id}.json")
}

fn valid_adhoc_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

async fn read_adhoc(
    store: &dyn ObjectStore,
    tenant: &TenantId,
    id: &str,
) -> Result<AdHocProfile, ConnectError> {
    if !valid_adhoc_id(id) {
        return Err(admin_error(
            Code::InvalidArgument,
            format!("id '{id}' is invalid"),
        ));
    }
    let object = store
        .get(&Path::from(adhoc_key(tenant, id)))
        .await
        .map_err(|error| match error {
            object_store::Error::NotFound { .. } => {
                admin_error(Code::NotFound, format!("profile {id} not found"))
            }
            error => admin_error(Code::Internal, error.to_string()),
        })?;
    let bytes = object
        .bytes()
        .await
        .map_err(|error| admin_error(Code::Internal, error.to_string()))?;
    serde_json::from_slice(&bytes).map_err(|error| {
        admin_error(
            Code::Internal,
            format!("stored profile is malformed: {error}"),
        )
    })
}

pub(crate) async fn upload_adhoc_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::adhocprofiles::v1::AdHocProfilesUploadRequest>,
) -> Result<ConnectResponse<pb::adhocprofiles::v1::AdHocProfilesGetResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    if req.0.profile.len() > ADHOC_MAX_BYTES.saturating_mul(4) / 3 + 4 {
        return Err(admin_error(
            Code::InvalidArgument,
            "profile payload exceeds 100 MiB",
        ));
    }
    let name = sanitize_name(&req.0.name);
    let uploaded_at = now_millis();
    let id = format!("{}-{name}", generated_letters());
    let profile = AdHocProfile {
        name,
        profile: req.0.profile,
        uploaded_at,
    };
    let response = adhoc_response(
        &id,
        &profile,
        None,
        state.effective_max_nodes(&tenant, req.0.max_nodes.unwrap_or_default()),
    )?;
    let bytes = serde_json::to_vec(&profile)
        .map_err(|error| admin_error(Code::Internal, error.to_string()))?;
    state
        .admin_store
        .put(&Path::from(adhoc_key(&tenant, &id)), bytes.into())
        .await
        .map_err(|error| admin_error(Code::Internal, error.to_string()))?;
    Ok(ConnectResponse::new(response))
}

pub(crate) async fn get_adhoc_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::adhocprofiles::v1::AdHocProfilesGetRequest>,
) -> Result<ConnectResponse<pb::adhocprofiles::v1::AdHocProfilesGetResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    let profile = read_adhoc(state.admin_store.as_ref(), &tenant, &req.0.id).await?;
    Ok(ConnectResponse::new(adhoc_response(
        &req.0.id,
        &profile,
        req.0.profile_type.as_deref(),
        state.effective_max_nodes(&tenant, req.0.max_nodes.unwrap_or_default()),
    )?))
}

pub(crate) async fn list_adhoc_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    _req: ConnectRequest<pb::adhocprofiles::v1::AdHocProfilesListRequest>,
) -> Result<ConnectResponse<pb::adhocprofiles::v1::AdHocProfilesListResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    let prefix = format!("{ADMIN_PREFIX}/{tenant}/adhoc/");
    let objects = state
        .admin_store
        .list(Some(&Path::from(prefix)))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| admin_error(Code::Internal, error.to_string()))?;
    let mut profiles = Vec::new();
    for object in objects {
        let id = object
            .location
            .filename()
            .and_then(|filename| filename.strip_suffix(".json"))
            .unwrap_or_default();
        if !valid_adhoc_id(id) {
            continue;
        }
        let profile = read_adhoc(state.admin_store.as_ref(), &tenant, id).await?;
        profiles.push(pb::adhocprofiles::v1::AdHocProfilesProfileMetadata {
            id: id.to_string(),
            name: profile.name,
            uploaded_at: profile.uploaded_at,
        });
    }
    profiles.sort_by_key(|profile| std::cmp::Reverse(profile.uploaded_at));
    Ok(ConnectResponse::new(
        pb::adhocprofiles::v1::AdHocProfilesListResponse { profiles },
    ))
}

pub(crate) async fn diff_adhoc_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::adhocprofiles::v1::AdHocProfilesDiffRequest>,
) -> Result<ConnectResponse<pb::adhocprofiles::v1::AdHocProfilesDiffResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    let left = read_adhoc(state.admin_store.as_ref(), &tenant, &req.0.left_id).await?;
    let right = read_adhoc(state.admin_store.as_ref(), &tenant, &req.0.right_id).await?;
    let left = decode_profile(&left.profile)?;
    let right = decode_profile(&right.profile)?;
    let left_types = profile_types(&left);
    let right_types: BTreeSet<_> = profile_types(&right).into_iter().collect();
    let common: Vec<_> = left_types
        .into_iter()
        .filter(|profile_type| right_types.contains(profile_type))
        .collect();
    let selected = req
        .0
        .profile_type
        .as_deref()
        .filter(|selected| common.iter().any(|candidate| candidate == selected))
        .or_else(|| common.first().map(String::as_str))
        .ok_or_else(|| {
            admin_error(
                Code::InvalidArgument,
                "profiles have no common profile types",
            )
        })?;
    let max_nodes = state.effective_max_nodes(&tenant, req.0.max_nodes.unwrap_or_default());
    let (left_tree, unit) = profile_tree(&left, selected)?;
    let (right_tree, _) = profile_tree(&right, selected)?;
    let flamebearer_profile = diff_json(
        diff_trees(&left_tree, &right_tree, max_nodes),
        selected,
        &unit,
    )?;
    Ok(ConnectResponse::new(
        pb::adhocprofiles::v1::AdHocProfilesDiffResponse {
            profile_types: common,
            flamebearer_profile,
        },
    ))
}

fn adhoc_response(
    id: &str,
    stored: &AdHocProfile,
    selected: Option<&str>,
    max_nodes: i64,
) -> Result<pb::adhocprofiles::v1::AdHocProfilesGetResponse, ConnectError> {
    let profile = decode_profile(&stored.profile)?;
    let profile_types = profile_types(&profile);
    let selected = selected
        .filter(|selected| profile_types.iter().any(|candidate| candidate == selected))
        .or_else(|| profile_types.first().map(String::as_str))
        .ok_or_else(|| admin_error(Code::InvalidArgument, "profile has no sample types"))?
        .to_string();
    let (tree, unit) = profile_tree(&profile, &selected)?;
    Ok(pb::adhocprofiles::v1::AdHocProfilesGetResponse {
        id: id.to_string(),
        name: stored.name.clone(),
        uploaded_at: stored.uploaded_at,
        profile_type: selected.clone(),
        profile_types,
        flamebearer_profile: flamegraph_json(tree.to_flamegraph(max_nodes), &selected, &unit)?,
    })
}

fn decode_profile(encoded: &str) -> Result<PprofProfile, ConnectError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| {
            admin_error(
                Code::InvalidArgument,
                format!("profile is not base64: {error}"),
            )
        })?;
    let bytes = if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut decoded = Vec::new();
        GzDecoder::new(bytes.as_slice())
            .take(u64::try_from(ADHOC_MAX_BYTES).unwrap_or(u64::MAX) + 1)
            .read_to_end(&mut decoded)
            .map_err(|error| {
                admin_error(
                    Code::InvalidArgument,
                    format!("invalid gzip profile: {error}"),
                )
            })?;
        decoded
    } else {
        bytes
    };
    if bytes.len() > ADHOC_MAX_BYTES {
        return Err(admin_error(
            Code::InvalidArgument,
            "profile exceeds 100 MiB",
        ));
    }
    PprofProfile::decode(&bytes).map_err(|error| {
        admin_error(
            Code::InvalidArgument,
            format!("invalid pprof profile: {error}"),
        )
    })
}

fn profile_types(profile: &PprofProfile) -> Vec<String> {
    profile
        .sample_types()
        .into_iter()
        .map(|(sample_type, _)| sample_type)
        .collect()
}

fn profile_tree(profile: &PprofProfile, selected: &str) -> Result<(Tree, String), ConnectError> {
    let sample_types = profile.sample_types();
    let index = sample_types
        .iter()
        .position(|(sample_type, _)| sample_type == selected)
        .ok_or_else(|| {
            admin_error(
                Code::InvalidArgument,
                format!("profile type {selected} not found"),
            )
        })?;
    let mut tree = Tree::new();
    for sample in profile.samples() {
        let value = sample.value.get(index).copied().unwrap_or_default();
        let frames: Vec<_> = profile
            .stack_frames(sample)
            .into_iter()
            .map(|function| Frame {
                function: function.to_string(),
                file: String::new(),
                line: 0,
            })
            .collect();
        tree.add_stack(&frames, value);
    }
    Ok((tree, sample_types[index].1.clone()))
}

fn flamegraph_json(
    graph: krabka_pprof::FlameGraph,
    profile_type: &str,
    unit: &str,
) -> Result<String, ConnectError> {
    serde_json::to_string(&json!({
        "flamebearer": {
            "names": graph.names,
            "levels": graph.levels.into_iter().map(|level| level.values).collect::<Vec<_>>(),
            "numTicks": graph.total,
            "maxSelf": graph.max_self,
        },
        "metadata": { "format": "single", "spyName": "", "sampleRate": 100, "units": unit, "name": profile_type }
    }))
    .map_err(|error| admin_error(Code::Internal, error.to_string()))
}

fn diff_json(
    graph: krabka_pprof::FlameGraphDiff,
    profile_type: &str,
    unit: &str,
) -> Result<String, ConnectError> {
    let max_self = graph
        .levels
        .iter()
        .flat_map(|level| level.values.chunks_exact(7))
        .fold(0_i64, |max, bar| max.max(bar[2]).max(bar[5]));
    serde_json::to_string(&json!({
        "flamebearer": {
            "names": graph.names,
            "levels": graph.levels.into_iter().map(|level| level.values).collect::<Vec<_>>(),
            "numTicks": graph.left_ticks + graph.right_ticks,
            "maxSelf": max_self,
            "leftTicks": graph.left_ticks,
            "rightTicks": graph.right_ticks,
        },
        "metadata": { "format": "double", "spyName": "", "sampleRate": 100, "units": unit, "name": profile_type }
    }))
    .map_err(|error| admin_error(Code::Internal, error.to_string()))
}

fn debuginfo_prefix(tenant: &TenantId, build_id: &str) -> String {
    format!("debug-info/{tenant}/{build_id}")
}

fn valid_build_id(build_id: &str) -> bool {
    (2..=40).contains(&build_id.len()) && build_id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn debuginfo_metadata(
    store: &dyn ObjectStore,
    tenant: &TenantId,
    build_id: &str,
) -> Result<Option<pb::debuginfo::v1alpha1::ObjectMetadata>, ConnectError> {
    read_message(
        store,
        &format!("{}/metadata", debuginfo_prefix(tenant, build_id)),
    )
    .await
}

pub(crate) async fn should_initiate_upload_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::debuginfo::v1alpha1::ShouldInitiateUploadRequest>,
) -> Result<ConnectResponse<pb::debuginfo::v1alpha1::ShouldInitiateUploadResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    let file = req
        .0
        .file
        .ok_or_else(|| admin_error(Code::InvalidArgument, "file metadata is required"))?;
    if file.gnu_build_id.is_empty() {
        return Ok(ConnectResponse::new(
            pb::debuginfo::v1alpha1::ShouldInitiateUploadResponse {
                should_initiate_upload: false,
                reason: EMPTY_BUILD_ID.to_string(),
            },
        ));
    }
    if !valid_build_id(&file.gnu_build_id) {
        return Err(admin_error(Code::InvalidArgument, "invalid gnu_build_id"));
    }
    if !matches!(file.r#type, 1 | 2) {
        return Err(admin_error(
            Code::InvalidArgument,
            "file type must be TYPE_EXECUTABLE_FULL or TYPE_EXECUTABLE_NO_TEXT",
        ));
    }
    let existing =
        debuginfo_metadata(state.admin_store.as_ref(), &tenant, &file.gnu_build_id).await?;
    let (should_upload, reason) = match existing.as_ref() {
        None => (true, FIRST_SEEN),
        Some(metadata)
            if metadata.state == 0
                && metadata.started_at.as_ref().is_some_and(|stamp| {
                    now_seconds().saturating_sub(stamp.seconds) >= UPLOAD_STALE_SECS
                }) =>
        {
            (true, UPLOAD_STALE)
        }
        Some(metadata) if metadata.state == 0 => (false, UPLOAD_IN_PROGRESS),
        Some(_) => (false, ALREADY_EXISTS),
    };
    if should_upload {
        if reason == UPLOAD_STALE {
            state.metrics.debuginfo_upload_retries.inc();
        }
        let metadata = pb::debuginfo::v1alpha1::ObjectMetadata {
            file: Some(file.clone()),
            state: pb::debuginfo::v1alpha1::object_metadata::State::Uploading as i32,
            started_at: Some(pbjson_types::Timestamp {
                seconds: now_seconds(),
                nanos: 0,
            }),
            finished_at: None,
            size_bytes: 0,
        };
        write_message(
            state.admin_store.as_ref(),
            &format!("{}/metadata", debuginfo_prefix(&tenant, &file.gnu_build_id)),
            &metadata,
        )
        .await?;
    }
    Ok(ConnectResponse::new(
        pb::debuginfo::v1alpha1::ShouldInitiateUploadResponse {
            should_initiate_upload: should_upload,
            reason: reason.to_string(),
        },
    ))
}

pub(crate) async fn upload_debuginfo_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    AxumPath(build_id): AxumPath<String>,
    request: Request,
) -> Response {
    let result = async {
        let headers = request.headers().clone();
        let tenant = tenant_from_headers(&headers, &state.tenant_policy)
            .map_err(|_| (StatusCode::BAD_REQUEST, "invalid tenant"))?;
        authorize_tenant(&principal, &tenant)
            .map_err(|_| (StatusCode::FORBIDDEN, "tenant access denied"))?;
        if !valid_build_id(&build_id) {
            return Err((StatusCode::BAD_REQUEST, "invalid gnu_build_id"));
        }
        let metadata = debuginfo_metadata(state.admin_store.as_ref(), &tenant, &build_id)
            .await
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal error"))?;
        if metadata.is_none_or(|metadata| metadata.state != 0) {
            return Err((
                StatusCode::PRECONDITION_FAILED,
                "no pending upload for this build ID",
            ));
        }
        let body = match tokio::time::timeout(
            std::time::Duration::from_mins(2),
            axum::body::to_bytes(request.into_body(), DEBUGINFO_MAX_BYTES),
        )
        .await
        {
            Ok(Ok(body)) => body,
            Ok(Err(_)) => {
                return Err((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "debug info upload is too large",
                ));
            }
            Err(_) => {
                state.metrics.debuginfo_upload_timeouts.inc();
                return Err((StatusCode::REQUEST_TIMEOUT, "debug info upload timed out"));
            }
        };
        state
            .admin_store
            .put(
                &Path::from(format!("{}/exe", debuginfo_prefix(&tenant, &build_id))),
                body.into(),
            )
            .await
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "upload failed"))?;
        Ok(StatusCode::OK)
    }
    .await;
    match result {
        Ok(status) => status.into_response(),
        Err((status, message)) => (status, message).into_response(),
    }
}

pub(crate) async fn upload_finished_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::debuginfo::v1alpha1::UploadFinishedRequest>,
) -> Result<ConnectResponse<pb::debuginfo::v1alpha1::UploadFinishedResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    if !valid_build_id(&req.0.gnu_build_id) {
        return Err(admin_error(Code::InvalidArgument, "invalid gnu_build_id"));
    }
    let mut metadata = debuginfo_metadata(state.admin_store.as_ref(), &tenant, &req.0.gnu_build_id)
        .await?
        .ok_or_else(|| {
            admin_error(
                Code::FailedPrecondition,
                "no pending upload for this build ID",
            )
        })?;
    if metadata.state != 0 {
        return Err(admin_error(
            Code::FailedPrecondition,
            "upload is not pending",
        ));
    }
    let object = state
        .admin_store
        .head(&Path::from(format!(
            "{}/exe",
            debuginfo_prefix(&tenant, &req.0.gnu_build_id)
        )))
        .await
        .map_err(|error| match error {
            object_store::Error::NotFound { .. } => {
                admin_error(Code::FailedPrecondition, "uploaded debug info is missing")
            }
            error => admin_error(Code::Internal, error.to_string()),
        })?;
    metadata.state = pb::debuginfo::v1alpha1::object_metadata::State::Uploaded as i32;
    metadata.finished_at = Some(pbjson_types::Timestamp {
        seconds: now_seconds(),
        nanos: 0,
    });
    metadata.size_bytes = i64::try_from(object.size).unwrap_or(i64::MAX);
    write_message(
        state.admin_store.as_ref(),
        &format!(
            "{}/metadata",
            debuginfo_prefix(&tenant, &req.0.gnu_build_id)
        ),
        &metadata,
    )
    .await?;
    Ok(ConnectResponse::new(
        pb::debuginfo::v1alpha1::UploadFinishedResponse {},
    ))
}

pub(crate) async fn list_debuginfo_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    _req: ConnectRequest<pb::debuginfo::v1alpha1::ListDebuginfoRequest>,
) -> Result<ConnectResponse<pb::debuginfo::v1alpha1::ListDebuginfoResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    let prefix = format!("debug-info/{tenant}/");
    let objects = state
        .admin_store
        .list(Some(&Path::from(prefix)))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| admin_error(Code::Internal, error.to_string()))?;
    let mut metadata = Vec::new();
    for object in objects {
        if !object.location.as_ref().ends_with("/metadata") {
            continue;
        }
        if let Some(value) =
            read_message(state.admin_store.as_ref(), object.location.as_ref()).await?
        {
            metadata.push(value);
        }
    }
    metadata.sort_by(|left: &pb::debuginfo::v1alpha1::ObjectMetadata, right| {
        left.file
            .as_ref()
            .map(|file| &file.gnu_build_id)
            .cmp(&right.file.as_ref().map(|file| &file.gnu_build_id))
    });
    Ok(ConnectResponse::new(
        pb::debuginfo::v1alpha1::ListDebuginfoResponse { object: metadata },
    ))
}

pub(crate) async fn delete_debuginfo_handler<S: ProfileStore>(
    Extension(state): Extension<std::sync::Arc<QuerierState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    req: ConnectRequest<pb::debuginfo::v1alpha1::DeleteDebuginfoRequest>,
) -> Result<ConnectResponse<pb::debuginfo::v1alpha1::DeleteDebuginfoResponse>, ConnectError> {
    let tenant = tenant(&state, &principal, &headers)?;
    if !valid_build_id(&req.0.gnu_build_id) {
        return Err(admin_error(Code::InvalidArgument, "invalid gnu_build_id"));
    }
    let prefix = debuginfo_prefix(&tenant, &req.0.gnu_build_id);
    delete_object(state.admin_store.as_ref(), &format!("{prefix}/metadata")).await?;
    delete_object(state.admin_store.as_ref(), &format!("{prefix}/exe")).await?;
    Ok(ConnectResponse::new(
        pb::debuginfo::v1alpha1::DeleteDebuginfoResponse {},
    ))
}

fn generated_letters() -> String {
    let now = u64::try_from(now_millis()).unwrap_or_default();
    let mut value = now
        .saturating_mul(1024)
        .saturating_add(NEXT_ID.fetch_add(1, Ordering::Relaxed) & 1023);
    let mut id = [b'a'; 10];
    for byte in id.iter_mut().rev() {
        *byte = b'a' + u8::try_from(value % 26).unwrap_or_default();
        value /= 26;
    }
    String::from_utf8_lossy(&id).into_owned()
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}

fn now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        })
}
