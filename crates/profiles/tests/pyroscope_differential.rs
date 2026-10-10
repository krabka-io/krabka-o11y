//! Docker-backed compatibility probes for real Pyroscope/Grafana surfaces.
//!
//! These tests are ignored by default because they pull and run upstream Docker
//! images. Run them explicitly with:
//!
//! `cargo test -p krabka-profiles --test pyroscope_differential -- --ignored`

use std::{
    collections::BTreeSet,
    io::Write,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use flate2::{Compression, write::GzEncoder};
use generated_differential::{LabelMatcher, MatchOp, TypedConstructor, TypedExpr};
use krabka_blockstore::TenantPolicy;
use krabka_observability::server_security::ServerSecurity;
use krabka_pprof::{PprofProfile, UnionProfileStore, proto};
use krabka_profiles::{
    ProfileRecord, ProfilesError,
    distributor::{self, DistributorState, WalSink},
    hot_store::WalTailProfileStore,
    limits::{Limits, OverridesProvider},
    query::{self, QuerierState},
    wire::pb,
};
use pinned_grafana_image::pinned_grafana_image;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use synthetic_cpu_profile::{FUNC_HOT, FUNC_WORK};
use testcontainers::{
    GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::oneshot;

#[path = "../../metrics-service/tests/support/generated_differential.rs"]
mod generated_differential;
#[path = "../../metrics-service/tests/support/pinned_grafana_image.rs"]
mod pinned_grafana_image;
#[path = "../src/bin/krabka-profiles/synthetic_cpu_profile.rs"]
mod synthetic_cpu_profile;

const TENANT: &str = "tenant-a";
/// Pyroscope HTTP port inside the container.
const PYROSCOPE_HTTP_PORT: u16 = 4040;
const PROFILE_ENV: &str = "pprofdiff";
const PROFILE_TYPE: &str = "goroutines:goroutine:count:goroutine:count";
const SELECTOR: &str = r#"{env="pprofdiff"}"#;

const TENANT_B: &str = "tenant-b";
const CPU_PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const WALL_PROFILE_TYPE: &str = "wall:wall:nanoseconds:wall:nanoseconds";
const CPU_NAME: &str = "process_cpu";
const E2E_SERVICE: &str = "checkout";
const E2E_SELECTOR: &str = r#"{service_name="checkout"}"#;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

#[derive(Clone, Default)]
struct CapturingSink {
    records: Arc<Mutex<Vec<ProfileRecord>>>,
    cold: WalTailProfileStore,
}

#[async_trait::async_trait]
impl WalSink for CapturingSink {
    async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError> {
        self.records
            .lock()
            .map_err(|_| ProfilesError::Wal("capturing sink lock poisoned".to_string()))?
            .push(rec);
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires the official Pyroscope 2.3.1 profilecli binary"]
async fn official_profilecli_uploads_elf_through_public_debuginfo_api() -> TestResult {
    let Ok(profilecli) = std::env::var("KRABKA_PROFILECLI") else {
        eprintln!("KRABKA_PROFILECLI is not set; skipping optional profilecli probe");
        return Ok(());
    };
    let krabka = start_krabka_pair(CapturingSink::default(), WalTailProfileStore::new()).await?;
    let url = krabka.querier_base.clone();
    let executable = profilecli.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(&profilecli)
            .args([
                "debuginfo",
                "upload",
                "--url",
                &url,
                "--tenant-id",
                TENANT,
                &executable,
            ])
            .output()
    })
    .await??;
    if !output.status.success() {
        krabka.shutdown();
        return Err(format!(
            "profilecli failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    let listed: Value = reqwest::Client::new()
        .post(format!(
            "{}/debuginfo.v1alpha1.DebuginfoService/ListDebuginfo",
            krabka.querier_base
        ))
        .header("content-type", "application/json")
        .header("connect-protocol-version", "1")
        .header("x-scope-orgid", TENANT)
        .body("{}")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        listed["object"]
            .as_array()
            .is_some_and(|objects| objects.len() == 1 && objects[0]["sizeBytes"] != "0"),
        "{listed}"
    );
    krabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/pyroscope image"]
async fn real_pyroscope_render_matches_krabka_after_identical_ingest() -> TestResult {
    let client = reqwest::Client::new();
    // Pin the storage architecture for the query-start bucket contract. The
    // auto migration router rewrites Start at its metastore split.
    // https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/pkg/frontend/readpath/router.go#L115-L124
    let pyroscope =
        start_pyroscope_with_options(&["-architecture.storage=v1", "-write-path=ingester"]).await?;
    let pyroscope_base = ready_pyroscope_base(&client, &pyroscope).await?;
    let fixture_nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?
        / 10_000_000_000
        * 10_000_000_000
        - 60_000_000_000;
    let fixture_timestamp = fixture_nanos / 1_000_000;
    let gzipped_pprof = timestamp_goroutine_profile(
        &fetch_goroutine_pprof(&client, &pyroscope_base).await?,
        fixture_nanos,
    )?;

    let krabka = ingest_into_both(&client, &pyroscope_base, &gzipped_pprof).await?;

    let pyroscope_render = render_until_non_empty(
        &client,
        &pyroscope_base,
        &[format!("{PROFILE_TYPE}{SELECTOR}")],
        "now-1h",
        "now",
        None,
    )
    .await?;
    let krabka_render = render_any(
        &client,
        &krabka.querier_base,
        &[format!("{PROFILE_TYPE}{SELECTOR}")],
        "0",
        "9223372036854775807",
        Some(TENANT),
        false,
    )
    .await?;

    let cases = [("pyroscope", &pyroscope_render), ("krabka", &krabka_render)];
    for (backend, render) in cases {
        assert!(
            flame_ticks(render).is_some_and(|ticks| ticks > 0),
            "{backend} render must report positive ticks"
        );
        assert!(
            flame_names(render).contains("runtime/pprof.profileWriter"),
            "{backend} render must contain runtime/pprof.profileWriter"
        );
    }
    assert_flamebearer_equal(&pyroscope_render, &krabka_render)?;

    assert_profile_types_match(&client, &pyroscope_base, &krabka.querier_base).await?;
    assert_label_names_match(&client, &pyroscope_base, &krabka.querier_base).await?;
    assert_label_values_match(&client, &pyroscope_base, &krabka.querier_base, "env").await?;
    assert_select_merge_stacktraces_match(
        &client,
        &pyroscope_base,
        &krabka.querier_base,
        fixture_timestamp,
    )
    .await?;
    assert_select_merge_pprof_match(
        &client,
        &pyroscope_base,
        &krabka.querier_base,
        fixture_timestamp,
    )
    .await?;
    assert_async_request_contract(
        &client,
        &pyroscope_base,
        &krabka.querier_base,
        fixture_timestamp,
    )
    .await?;
    assert_select_series_match(
        &client,
        &pyroscope_base,
        &krabka.querier_base,
        fixture_timestamp,
    )
    .await?;
    assert_diff_match(
        &client,
        &pyroscope_base,
        &krabka.querier_base,
        fixture_timestamp,
    )
    .await?;

    assert_profile_types_contain(&client, &krabka.querier_base, Some(TENANT)).await?;
    assert_label_names_contain(&client, &krabka.querier_base, Some(TENANT)).await?;
    assert_label_values_contain(
        &client,
        &krabka.querier_base,
        Some(TENANT),
        "env",
        PROFILE_ENV,
    )
    .await?;
    assert_select_merge_stacktraces_has_symbol(&client, &krabka.querier_base, Some(TENANT)).await?;
    assert_select_series_has_points(&client, &krabka.querier_base, Some(TENANT)).await?;
    assert_select_heatmap_has_slots(&client, &krabka.querier_base, Some(TENANT)).await?;
    assert_diff_has_ticks(&client, &krabka.querier_base, Some(TENANT)).await?;

    krabka.shutdown();
    Ok(())
}

/// Differential coverage for the two RPCs that the grafana-pyroscope-app v2.0.7
/// "all services" drilldown grid issues before it decides whether to fan out
/// the per-panel time-series queries:
///
///   * `querier.v1.QuerierService/GetProfileStats` with an empty request body,
///     and
///   * `querier.v1.QuerierService/Series` with `matchers:[]` and
///     `labelNames:["service_name","__profile_type__"]`. As a control, the test
///     also issues the full-label-set form `labelNames:[]`.
///
/// If the drilldown shows "No data" on every panel and reports no JS error, the
/// most likely cause is a response shape deviation in one of these two RPCs.
/// Such a deviation makes the response parser of the app stop silently instead
/// of throw. This test ingests one identical goroutine profile into both real
/// Pyroscope and krabka, issues the same calls to both, and compares the
/// responses field by field. The compared fields are the JSON key casing, the
/// presence or absence of a spurious empty labelset, the label key NAMES and
/// ORDER within each set, the SET of `(service_name,__profile_type__)` tuples,
/// `int64`-as-string-vs-number for the `GetProfileStats` times, and any extra
/// or missing top-level fields. The test always prints both raw bodies under
/// `--nocapture`, so a deviation is quotable.
#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/pyroscope image"]
async fn real_pyroscope_series_and_stats_match_krabka_after_identical_ingest() -> TestResult {
    let client = reqwest::Client::new();
    let pyroscope = start_pyroscope().await?;
    let pyroscope_base = ready_pyroscope_base(&client, &pyroscope).await?;
    let gzipped_pprof = fetch_goroutine_pprof(&client, &pyroscope_base).await?;

    let krabka = ingest_into_both(&client, &pyroscope_base, &gzipped_pprof).await?;

    // (a) GetProfileStats — empty (all-default) request body. Pyroscope ingests
    // asynchronously, so poll until it reports data, then compare.
    let stats_body = json!({});
    let pyroscope_stats = connect_json_until(
        &client,
        &pyroscope_base,
        None,
        "GetProfileStats",
        stats_body.clone(),
        profile_stats_has_data,
    )
    .await?;
    let krabka_stats = connect_json_until(
        &client,
        &krabka.querier_base,
        Some(TENANT),
        "GetProfileStats",
        stats_body.clone(),
        profile_stats_has_data,
    )
    .await?;
    eprintln!("[GetProfileStats] pyroscope = {pyroscope_stats}");
    eprintln!("[GetProfileStats] krabka    = {krabka_stats}");
    assert_get_profile_stats_compatible(&pyroscope_stats, &krabka_stats)?;

    // The two backends do NOT ingest identical label sets: real Pyroscope also
    // self-instruments (a `service_name="pyroscope"` series tree), while krabka
    // holds only the one `service_name="api"` goroutine profile we pushed. So the
    // FULL set of (service_name,__profile_type__) tuples legitimately differs.
    // The comparison below is therefore scoped to the tuple BOTH backends share —
    // the ingested `(api, goroutines:...)` — plus the shape invariants that gate
    // the drilldown: wire-key casing, absence of a spurious empty label set, and
    // (the core regression) that krabka returns data for the drilldown's exact
    // call shape, which carries NO time range.
    let shared_tuple = vec![
        ("service_name".to_string(), "api".to_string()),
        ("__profile_type__".to_string(), PROFILE_TYPE.to_string()),
    ];

    // (b) Series with the EXACT body the grafana-pyroscope-app drilldown sends:
    // `matchers:[]`, `labelNames:[service_name,__profile_type__]`, and crucially
    // NO `start`/`end` (they default to 0). Real Pyroscope's Series is range-
    // agnostic and returns the full enumeration regardless; krabka must do the
    // same, or the drilldown sees zero services and never fans out panel queries.
    let drilldown_body = json!({
        "matchers": [],
        "labelNames": ["service_name", "__profile_type__"],
    });
    let pyroscope_drilldown = connect_json_until(
        &client,
        &pyroscope_base,
        None,
        "Series",
        drilldown_body.clone(),
        |value| series_contains_tuple(value, &shared_tuple),
    )
    .await?;
    // krabka is fed synchronously above, so a single call suffices; do not poll on
    // readiness here — that would mask the very "returns nothing for a no-range
    // request" regression this asserts.
    let krabka_drilldown = connect_json(
        &client,
        &krabka.querier_base,
        Some(TENANT),
        "Series",
        drilldown_body,
    )
    .await?;
    eprintln!("[Series drilldown no-range] pyroscope = {pyroscope_drilldown}");
    eprintln!("[Series drilldown no-range] krabka    = {krabka_drilldown}");
    assert_series_drilldown_compatible(&pyroscope_drilldown, &krabka_drilldown, &shared_tuple)?;

    // (c) Series with the same projection but an explicit wide range. Both should
    // still surface the shared tuple, and krabka must not emit a spurious empty
    // label set.
    let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
        .unwrap_or(i64::MAX);
    let ranged_body = json!({
        "matchers": [],
        "labelNames": ["service_name", "__profile_type__"],
        "start": now_ms - 3_600_000,
        "end": now_ms + 3_600_000,
    });
    let pyroscope_ranged = connect_json_until(
        &client,
        &pyroscope_base,
        None,
        "Series",
        ranged_body.clone(),
        |value| series_contains_tuple(value, &shared_tuple),
    )
    .await?;
    let krabka_ranged = connect_json(
        &client,
        &krabka.querier_base,
        Some(TENANT),
        "Series",
        ranged_body,
    )
    .await?;
    eprintln!("[Series projected ranged] pyroscope = {pyroscope_ranged}");
    eprintln!("[Series projected ranged] krabka    = {krabka_ranged}");
    assert_series_drilldown_compatible(&pyroscope_ranged, &krabka_ranged, &shared_tuple)?;

    // (d) Series with empty labelNames (full label sets) over the wide range. The
    // spurious-empty-labelset bug (`{"labelsSet":[{}]}`) reproduces here: krabka
    // inserts an empty projection when `labelNames` is empty. Assert no empty set
    // and that krabka returns the full label set for the shared `api` series, the
    // way Pyroscope does (autocomplete + the drilldown both rely on this).
    let full_body = json!({
        "matchers": [],
        "labelNames": [],
        "start": now_ms - 3_600_000,
        "end": now_ms + 3_600_000,
    });
    let pyroscope_full = connect_json_until(
        &client,
        &pyroscope_base,
        None,
        "Series",
        full_body.clone(),
        series_has_labelsets,
    )
    .await?;
    let krabka_full = connect_json(
        &client,
        &krabka.querier_base,
        Some(TENANT),
        "Series",
        full_body,
    )
    .await?;
    eprintln!("[Series full labelNames=[]] pyroscope = {pyroscope_full}");
    eprintln!("[Series full labelNames=[]] krabka    = {krabka_full}");
    assert_series_full_compatible(&pyroscope_full, &krabka_full)?;

    krabka.shutdown();
    Ok(())
}

/// `GetProfileStats` is "ready" once the backend reports any ingested data.
/// Canonical proto-JSON omits `dataIngested:false`, so a present and true
/// `dataIngested` is the readiness signal.
fn profile_stats_has_data(value: &Value) -> bool {
    value
        .get("dataIngested")
        .or_else(|| value.get("data_ingested"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// A `Series` response is "ready" once it carries at least one non-empty label
/// set under the `labelsSet` key. This function also accepts the `labels_set`
/// spelling.
fn series_has_labelsets(value: &Value) -> bool {
    series_label_sets(value).is_some_and(|sets| sets.iter().any(|set| !set.is_empty()))
}

/// Extracts the `Series` label sets as ordered `(name, value)` vectors and keeps
/// the on-the-wire key order within each set.
///
/// The function accepts the `labelsSet` key of canonical proto-JSON and
/// connect-go, or the `labels_set` spelling. It returns `None` only when
/// neither key is present. That state is itself a shape signal, and it differs
/// from "present but empty".
fn series_label_sets(value: &Value) -> Option<Vec<Vec<(String, String)>>> {
    let sets = value
        .get("labelsSet")
        .or_else(|| value.get("labels_set"))?
        .as_array()?;
    Some(
        sets.iter()
            .map(|entry| {
                entry
                    .get("labels")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flat_map(|labels| labels.iter())
                    .filter_map(|pair| {
                        let name = pair.get("name").and_then(Value::as_str)?;
                        // proto-JSON omits empty-string values; treat a missing
                        // value as the empty string so projection still records
                        // the key (an empty-VALUE label is itself a shape signal).
                        let val = pair.get("value").and_then(Value::as_str).unwrap_or("");
                        Some((name.to_string(), val.to_string()))
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),
    )
}

/// True if the `Series` response contains a label set that equals `tuple` as an
/// unordered `(name,value)` collection.
///
/// The test uses this both as a Pyroscope readiness predicate and as the krabka
/// assertion target. The comparison ignores order within the set, because
/// [`assert_series_key_order`] checks key order separately.
fn series_contains_tuple(value: &Value, tuple: &[(String, String)]) -> bool {
    let want = tuple.iter().cloned().collect::<BTreeSet<_>>();
    series_label_sets(value).is_some_and(|sets| {
        sets.iter()
            .any(|set| set.iter().cloned().collect::<BTreeSet<_>>() == want)
    })
}

/// Compares a projected `Series` response between Pyroscope and krabka.
///
/// A projected response comes from the drilldown's
/// `labelNames=[service_name,__profile_type__]` call. The comparison covers the
/// axes that gate the drilldown:
///   1. the wire key, `labelsSet` or `labels_set` or absent,
///   2. absence of a spurious empty `{}` label set on the krabka side,
///   3. the shared `(api, goroutines:...)` tuple is present on BOTH. This is
///      the core regression: krabka must return it even with no time range.
///   4. the per-set key ORDER agrees for that shared tuple.
///
/// The comparison deliberately leaves out the FULL set of tuples. Real
/// Pyroscope also self-instruments, so its enumeration is a strict superset of
/// krabka's.
fn assert_series_drilldown_compatible(
    pyroscope: &Value,
    krabka: &Value,
    shared_tuple: &[(String, String)],
) -> TestResult {
    let py_key = series_wire_key(pyroscope);
    let cr_key = series_wire_key(krabka);
    if py_key != cr_key {
        return Err(format!(
            "Series(projected): label-set wire key differs: pyroscope={py_key:?} krabka={cr_key:?}\n  pyroscope={pyroscope}\n  krabka={krabka}"
        )
        .into());
    }

    let cr_sets = series_label_sets(krabka).ok_or_else(|| {
        format!("Series(projected): krabka response missing label-set array: {krabka}")
    })?;
    let cr_empty = cr_sets.iter().filter(|set| set.is_empty()).count();
    if cr_empty != 0 {
        return Err(format!(
            "Series(projected): krabka emitted {cr_empty} spurious empty label set(s): {krabka}"
        )
        .into());
    }

    if !series_contains_tuple(pyroscope, shared_tuple) {
        return Err(format!(
            "Series(projected): pyroscope missing shared tuple {shared_tuple:?}: {pyroscope}"
        )
        .into());
    }
    if !series_contains_tuple(krabka, shared_tuple) {
        return Err(format!(
            "Series(projected): krabka missing shared tuple {shared_tuple:?} (drilldown sees no service → \"No data\"): {krabka}"
        )
        .into());
    }

    assert_series_key_order("Series(projected)", pyroscope, krabka, shared_tuple)
}

/// Compares a full-label-set `Series` response between the two backends.
///
/// The full-label-set form is `labelNames=[]`. The check confirms that neither
/// side has a spurious empty set, and that krabka returns the full label set
/// for the shared `api` series and not a projection or an empty set.
fn assert_series_full_compatible(pyroscope: &Value, krabka: &Value) -> TestResult {
    let py_key = series_wire_key(pyroscope);
    let cr_key = series_wire_key(krabka);
    if py_key != cr_key {
        return Err(format!(
            "Series(full): label-set wire key differs: pyroscope={py_key:?} krabka={cr_key:?}\n  pyroscope={pyroscope}\n  krabka={krabka}"
        )
        .into());
    }

    let py_sets = series_label_sets(pyroscope).ok_or_else(|| {
        format!("Series(full): pyroscope response missing label-set array: {pyroscope}")
    })?;
    let cr_sets = series_label_sets(krabka).ok_or_else(|| {
        format!("Series(full): krabka response missing label-set array: {krabka}")
    })?;

    let py_empty = py_sets.iter().filter(|set| set.is_empty()).count();
    let cr_empty = cr_sets.iter().filter(|set| set.is_empty()).count();
    if py_empty != 0 {
        return Err(format!(
            "Series(full): pyroscope unexpectedly emitted {py_empty} empty label set(s): {pyroscope}"
        )
        .into());
    }
    if cr_empty != 0 {
        return Err(format!(
            "Series(full): krabka emitted {cr_empty} spurious empty label set(s): {krabka}"
        )
        .into());
    }

    // krabka must surface the ingested `api` series with its full label set,
    // including `service_name`, `__name__`, `env`, and `__profile_type__`.
    let krabka_api = cr_sets
        .iter()
        .find(|set| {
            set.iter()
                .any(|(name, value)| name == "service_name" && value == "api")
        })
        .ok_or_else(|| {
            format!("Series(full): krabka missing api series in full label sets: {krabka}")
        })?;
    for required in ["service_name", "__name__", "env", "__profile_type__"] {
        if !krabka_api.iter().any(|(name, _)| name == required) {
            return Err(format!(
                "Series(full): krabka api label set missing `{required}`: {krabka_api:?}"
            )
            .into());
        }
    }
    Ok(())
}

/// Asserts that the per-set key ORDER of the shared tuple matches between the
/// two backends. Both sides project onto the requested `labelNames`, so the
/// on-the-wire key order must be identical.
fn assert_series_key_order(
    label: &str,
    pyroscope: &Value,
    krabka: &Value,
    shared_tuple: &[(String, String)],
) -> TestResult {
    let want = shared_tuple.iter().cloned().collect::<BTreeSet<_>>();
    let find_keys = |value: &Value| -> Option<Vec<String>> {
        series_label_sets(value)?.into_iter().find_map(|set| {
            (set.iter().cloned().collect::<BTreeSet<_>>() == want)
                .then(|| set.iter().map(|(name, _)| name.clone()).collect())
        })
    };
    let py_order = find_keys(pyroscope);
    let cr_order = find_keys(krabka);
    if py_order != cr_order {
        return Err(format!(
            "{label}: key order for shared tuple differs: pyroscope={py_order:?} krabka={cr_order:?}\n  pyroscope={pyroscope}\n  krabka={krabka}"
        )
        .into());
    }
    Ok(())
}

/// Which top-level key carries the label sets, for a casing-sensitive diff.
fn series_wire_key(value: &Value) -> Option<&'static str> {
    if value.get("labelsSet").is_some() {
        Some("labelsSet")
    } else if value.get("labels_set").is_some() {
        Some("labels_set")
    } else {
        None
    }
}

/// Compares `GetProfileStats` between Pyroscope and krabka.
///
/// The Drilldown gates only on a truthy `dataIngested`. It uses the time window
/// to seed a default range, not to decide whether to fan out panel queries.
/// This function therefore checks the axes that can break the parser of the app
/// or its gate:
///   1. the casing of the `dataIngested` wire key and its truthiness, and
///   2. for any time field present on both sides, the
///      int64-as-string-vs-number JSON representation.
///
/// Two states are deliberately NOT a failure: a time field present on one side
/// and omitted on the other, and different timestamp magnitudes. Canonical
/// proto-JSON omits fields equal to their zero default, and the goroutine pprof
/// carries no `time_nanos`. The two backends therefore disagree legitimately on
/// whether `oldestProfileTime` is 0, which proto-JSON omits, or the ingest
/// instant. A `0`-vs-now `oldestProfileTime` does not stop the drilldown from
/// issuing panel queries, so a hard failure there would be a false positive.
/// The always-on `eprintln!` of both raw bodies still shows the deviation.
fn assert_get_profile_stats_compatible(pyroscope: &Value, krabka: &Value) -> TestResult {
    let py_obj = pyroscope
        .as_object()
        .ok_or_else(|| format!("GetProfileStats: pyroscope response not an object: {pyroscope}"))?;
    let cr_obj = krabka
        .as_object()
        .ok_or_else(|| format!("GetProfileStats: krabka response not an object: {krabka}"))?;

    // 1. dataIngested: same wire key (casing), both truthy.
    let py_ingested_key = stats_field_key(py_obj, "dataIngested", "data_ingested");
    let cr_ingested_key = stats_field_key(cr_obj, "dataIngested", "data_ingested");
    if py_ingested_key != cr_ingested_key {
        return Err(format!(
            "GetProfileStats: dataIngested wire key differs: pyroscope={py_ingested_key:?} krabka={cr_ingested_key:?}\n  pyroscope={pyroscope}\n  krabka={krabka}"
        )
        .into());
    }
    let py_ingested = stats_truthy(py_obj, "dataIngested", "data_ingested");
    let cr_ingested = stats_truthy(cr_obj, "dataIngested", "data_ingested");
    if py_ingested != cr_ingested {
        return Err(format!(
            "GetProfileStats: dataIngested truthiness differs: pyroscope={py_ingested} krabka={cr_ingested}\n  pyroscope={pyroscope}\n  krabka={krabka}"
        )
        .into());
    }

    // 2. oldest/newest time fields: where present on BOTH sides, the JSON
    // representation (string vs number) must agree. Presence itself is not
    // required to match (proto3 zero-omission is canonical on both backends).
    for (camel, snake) in [
        ("oldestProfileTime", "oldest_profile_time"),
        ("newestProfileTime", "newest_profile_time"),
    ] {
        let py_repr =
            stats_field_key(py_obj, camel, snake).and_then(|key| json_number_repr(&py_obj[key]));
        let cr_repr =
            stats_field_key(cr_obj, camel, snake).and_then(|key| json_number_repr(&cr_obj[key]));
        if let (Some(py_repr), Some(cr_repr)) = (py_repr, cr_repr)
            && py_repr != cr_repr
        {
            return Err(format!(
                "GetProfileStats: {camel} JSON representation differs: pyroscope={py_repr:?} krabka={cr_repr:?}\n  pyroscope={pyroscope}\n  krabka={krabka}"
            )
            .into());
        }
    }

    Ok(())
}

/// Which of the `camelCase` / `snake_case` spellings of a stats field is present.
fn stats_field_key(
    obj: &serde_json::Map<String, Value>,
    camel: &'static str,
    snake: &'static str,
) -> Option<&'static str> {
    if obj.contains_key(camel) {
        Some(camel)
    } else if obj.contains_key(snake) {
        Some(snake)
    } else {
        None
    }
}

/// A stats boolean is truthy if it is present and `true`, or present as a
/// non-zero number.
///
/// Pyroscope's proto types `data_ingested` as a bool. This function also
/// accepts a numeric encoding, so an int-vs-bool deviation shows as a
/// representation difference and not as a crash.
fn stats_truthy(
    obj: &serde_json::Map<String, Value>,
    camel: &'static str,
    snake: &'static str,
) -> bool {
    let Some(key) = stats_field_key(obj, camel, snake) else {
        return false;
    };
    let value = &obj[key];
    value.as_bool().unwrap_or(false)
        || value.as_i64().is_some_and(|n| n != 0)
        || value.as_str().is_some_and(|s| s == "true" || s == "1")
}

/// Classifies a JSON number-like value as `"string"` or `"number"`. The
/// classification catches deviations between int64-as-string, which canonical
/// proto-JSON uses, and int64-as-number.
fn json_number_repr(value: &Value) -> Option<&'static str> {
    if value.is_string() {
        Some("string")
    } else if value.is_number() {
        Some("number")
    } else {
        None
    }
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/grafana image"]
async fn grafana_accepts_pyroscope_datasource_pointing_at_krabka() -> TestResult {
    let client = reqwest::Client::new();
    let sink = CapturingSink::default();
    let store = WalTailProfileStore::new();
    let krabka = start_krabka_pair(sink, store).await?;

    let grafana = start_grafana().await?;
    let grafana_base = mapped_base_url(&grafana, 3000).await?;
    wait_for_http_ok(&client, &grafana_base, &["/api/health"]).await?;

    let payload = json!({
        "name": "Krabka Profiles",
        "type": "grafana-pyroscope-datasource",
        "access": "proxy",
        "url": krabka.querier_base,
        "isDefault": true,
        "jsonData": {}
    });
    let created: Value = client
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth("admin", Some("admin"))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let uid = created
        .get("datasource")
        .and_then(|datasource| datasource.get("uid"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let id = created
        .get("datasource")
        .and_then(|datasource| datasource.get("id"))
        .and_then(Value::as_i64)
        .unwrap_or_default();

    let fetched: Value = if let Some(uid) = uid {
        client
            .get(format!("{grafana_base}/api/datasources/uid/{uid}"))
            .basic_auth("admin", Some("admin"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?
    } else if id != 0 {
        client
            .get(format!("{grafana_base}/api/datasources/id/{id}"))
            .basic_auth("admin", Some("admin"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?
    } else {
        let encoded = url::form_urlencoded::byte_serialize(b"Krabka Profiles").collect::<String>();
        client
            .get(format!("{grafana_base}/api/datasources/name/{encoded}"))
            .basic_auth("admin", Some("admin"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?
    };

    assert_eq!(
        fetched.get("type").and_then(Value::as_str),
        Some("grafana-pyroscope-datasource")
    );
    assert_eq!(
        fetched.get("url").and_then(Value::as_str),
        Some(krabka.querier_base.as_str())
    );

    krabka.shutdown();
    Ok(())
}

async fn start_pyroscope() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    start_pyroscope_with_options(&[]).await
}

async fn start_pyroscope_with_options(
    options: &[&str],
) -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    // No default. //bazel/defs.bzl sets this from //bazel/images/images.bzl,
    // the same map that decides what `docker load` tags. A default here would
    // be a second copy of that decision, and when the two disagreed
    // testcontainers pulled the image over the network and the suite compared
    // against whatever it got rather than against the pinned bytes.
    let tag = std::env::var("KRABKA_PYROSCOPE_IMAGE_TAG").expect(
        "KRABKA_PYROSCOPE_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under \
         cargo, set it to that image's tag in //bazel/images/images.bzl.",
    );
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/pyroscope".to_string(), tag)
            .with_exposed_port(PYROSCOPE_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::seconds(3))
            .with_cmd(
                std::iter::once("-config.file=/etc/pyroscope/config.yaml")
                    .chain(options.iter().copied()),
            )
            .start(),
    )
    .await??)
}

async fn start_grafana() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        pinned_grafana_image()
            .with_exposed_port(3000.tcp())
            .with_wait_for(WaitFor::seconds(5))
            .with_env_var("GF_PLUGINS_PREINSTALL_DISABLED", "true")
            .with_env_var("GF_SECURITY_ADMIN_PASSWORD", "admin")
            // Let the container reach the in-process Krabka querier on the host via
            // host.docker.internal (host-gateway mapping; works on Docker Desktop + Linux).
            .with_host("host.docker.internal", Host::HostGateway)
            .start(),
    )
    .await??)
}

async fn mapped_base_url(
    container: &testcontainers::ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port.tcp()).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}

struct KrabkaPair {
    distributor_base: String,
    querier_base: String,
    distributor_shutdown: Option<oneshot::Sender<()>>,
    querier_shutdown: Option<oneshot::Sender<()>>,
}

impl KrabkaPair {
    fn shutdown(mut self) {
        if let Some(tx) = self.distributor_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.querier_shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Starts a krabka pair, pushes `gzipped_pprof` to both it and the Pyroscope
/// at `pyroscope_base`, and drains krabka's WAL into its cold store.
async fn ingest_into_both(
    client: &reqwest::Client,
    pyroscope_base: &str,
    gzipped_pprof: &[u8],
) -> TestResult<KrabkaPair> {
    let sink = CapturingSink::default();
    let krabka = start_krabka_pair(sink.clone(), WalTailProfileStore::new()).await?;

    post_push_profile(client, PushTarget::oracle(pyroscope_base), gzipped_pprof).await?;
    post_push_profile(
        client,
        PushTarget::krabka(&krabka.distributor_base),
        gzipped_pprof,
    )
    .await?;
    drain_sink_into_cold_store(&sink)?;
    Ok(krabka)
}

async fn start_krabka_pair(
    sink: CapturingSink,
    store: WalTailProfileStore,
) -> TestResult<KrabkaPair> {
    start_krabka_pair_with_architecture(sink, store, query::PyroscopeQueryArchitecture::V1).await
}

async fn start_krabka_pair_with_architecture(
    sink: CapturingSink,
    store: WalTailProfileStore,
    architecture: query::PyroscopeQueryArchitecture,
) -> TestResult<KrabkaPair> {
    start_krabka_pair_with_query_options(
        sink,
        store,
        QuerierQueryMode {
            architecture,
            async_queries_enabled: architecture == query::PyroscopeQueryArchitecture::V2,
            query_analysis_series_enabled: true,
        },
    )
    .await
}

/// Serves a distributor on a loopback port whose WAL is `sink`. It runs until
/// the returned sender fires or is dropped.
async fn start_distributor(
    sink: CapturingSink,
) -> TestResult<(std::net::SocketAddr, oneshot::Sender<()>)> {
    let (distributor_shutdown, distributor_rx) = oneshot::channel();
    let distributor_state = Arc::new(DistributorState {
        sink: Arc::new(sink),
        overrides: OverridesProvider::new(Limits::default()),
        tenant_policy: TenantPolicy::anonymous(),
        active_series: Mutex::default(),
        cumulative_profiles: tokio::sync::Mutex::default(),
        ingestion_buckets: Mutex::default(),
        relabel: Vec::new(),
        max_decompressed: krabka_units::mebibytes(16),
        max_tracked_tenants: 4096,
        legacy_decode_limits: krabka_profiles::ingest::LegacyDecodeLimits::default(),
        metrics: krabka_profiles::metrics::ServiceMetrics::new(),
    });
    let distributor_addr = distributor::serve(
        "127.0.0.1:0".parse()?,
        distributor_state,
        &ServerSecurity::default(),
        async move {
            let _ = distributor_rx.await;
        },
    )
    .await?;
    Ok((distributor_addr, distributor_shutdown))
}

/// The hot store a querier reads in front of its cold one.
struct ProfileTiers {
    hot: WalTailProfileStore,
    cold: WalTailProfileStore,
}

/// A querier over `tiers`, with no per-query range cap. The differential /
/// e2e corpus intentionally queries the full `[0, i64::MAX]` range to compare
/// against real Pyroscope.
fn unbounded_querier_state(
    tiers: ProfileTiers,
) -> QuerierState<UnionProfileStore<WalTailProfileStore, WalTailProfileStore>> {
    QuerierState::new_with_limits(
        Arc::new(UnionProfileStore::new(
            Arc::new(tiers.hot),
            Arc::new(tiers.cold),
        )),
        krabka_profiles::limits::Limits {
            max_query_length: <krabka_units::Time as krabka_units::convert::TimeExt>::ZERO,
            ..Default::default()
        },
    )
}

/// How a Krabka querier answers queries.
#[derive(Clone, Copy)]
struct QuerierQueryMode {
    architecture: query::PyroscopeQueryArchitecture,
    async_queries_enabled: bool,
    query_analysis_series_enabled: bool,
}

async fn start_krabka_pair_with_query_options(
    sink: CapturingSink,
    store: WalTailProfileStore,
    query_mode: QuerierQueryMode,
) -> TestResult<KrabkaPair> {
    let cold = sink.cold.clone();
    let (distributor_addr, distributor_shutdown) = start_distributor(sink).await?;

    let (querier_shutdown, querier_rx) = oneshot::channel();
    let querier_state = Arc::new(
        unbounded_querier_state(ProfileTiers { hot: store, cold })
            .with_query_architecture(query_mode.architecture)
            .with_async_queries_enabled(query_mode.async_queries_enabled)
            .with_query_analysis_series_enabled(query_mode.query_analysis_series_enabled),
    );
    let querier_addr = query::serve(
        "127.0.0.1:0".parse()?,
        querier_state,
        &ServerSecurity::default(),
        async move {
            let _ = querier_rx.await;
        },
    )
    .await?;

    Ok(KrabkaPair {
        distributor_base: format!("http://{distributor_addr}"),
        querier_base: format!("http://{querier_addr}"),
        distributor_shutdown: Some(distributor_shutdown),
        querier_shutdown: Some(querier_shutdown),
    })
}

async fn fetch_goroutine_pprof(client: &reqwest::Client, base: &str) -> TestResult<Vec<u8>> {
    Ok(client
        .get(format!("{base}/debug/pprof/goroutine?debug=0"))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec())
}

fn timestamp_goroutine_profile(compressed: &[u8], timestamp_nanos: i64) -> TestResult<Vec<u8>> {
    use std::io::Read;

    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(compressed).read_to_end(&mut bytes)?;
    let mut profile: proto::Profile = PprofProfile::decode(&bytes)?.into();
    profile.time_nanos = timestamp_nanos;
    gzip_bytes(&PprofProfile::from(profile).encode())
}

/// A `push.v1` JSON push.
#[derive(Clone, Copy)]
struct PushJson<'a> {
    base: &'a str,
    /// Sent as `X-Scope-OrgID` when given.
    tenant: Option<&'a str>,
    body: &'a Value,
    /// Names the push in the error.
    what: &'a str,
}

/// Where a test push goes: the push API at `base`, as `tenant`.
#[derive(Clone, Copy)]
struct PushTarget<'a> {
    base: &'a str,
    /// Sent as `X-Scope-OrgID` when given.
    tenant: Option<&'a str>,
}

impl<'a> PushTarget<'a> {
    /// The upstream oracle at `base`, which takes no tenant.
    fn oracle(base: &'a str) -> Self {
        Self { base, tenant: None }
    }

    /// The Krabka distributor at `base`, as the test tenant.
    fn krabka(base: &'a str) -> Self {
        Self {
            base,
            tenant: Some(TENANT),
        }
    }
}

/// Posts `push` and fails unless it is accepted.
async fn post_push_json(client: &reqwest::Client, push: PushJson<'_>) -> TestResult {
    let PushJson {
        base,
        tenant,
        body,
        what,
    } = push;
    let request = client
        .post(format!("{base}/push.v1.PusherService/Push"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(body);
    send_expecting_ok(
        request,
        &ExpectedOk {
            tenant,
            what: &format!("push.v1 {what} to {base}"),
        },
    )
    .await
}

/// Who a request that must be accepted is sent as, and how a rejection of it
/// names the request.
struct ExpectedOk<'a> {
    tenant: Option<&'a str>,
    what: &'a str,
}

/// Sends `request`, as `expected.tenant` when there is one, and fails unless
/// the response is `200 OK`.
async fn send_expecting_ok(
    mut request: reqwest::RequestBuilder,
    expected: &ExpectedOk<'_>,
) -> TestResult {
    if let Some(tenant) = expected.tenant {
        request = request.header("x-scope-orgid", tenant);
    }
    let response = request.send().await?;
    let status = response.status();
    if status != StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("{} returned {status}: {body}", expected.what).into());
    }
    Ok(())
}

async fn post_push_profile(
    client: &reqwest::Client,
    target: PushTarget<'_>,
    gzipped_pprof: &[u8],
) -> TestResult {
    let PushTarget { base, tenant } = target;
    let body = json!({
        "series": [{
            "labels": [
                { "name": "__name__", "value": "goroutines" },
                { "name": "service_name", "value": "api" },
                { "name": "env", "value": PROFILE_ENV }
            ],
            "samples": [{
                "rawProfile": BASE64.encode(gzipped_pprof),
                "ID": "krabka-differential-goroutine"
            }]
        }]
    });
    post_push_json(
        client,
        PushJson {
            base,
            tenant,
            body: &body,
            what: "profile push",
        },
    )
    .await
}

async fn render_any(
    client: &reqwest::Client,
    base: &str,
    queries: &[String],
    from: &str,
    until: &str,
    tenant: Option<&str>,
    require_non_empty: bool,
) -> TestResult<Value> {
    let mut attempts = Vec::new();
    for path in ["/pyroscope/render", "/render"] {
        for query in queries {
            let encoded = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("query", query)
                .append_pair("from", from)
                .append_pair("until", until)
                .finish();
            let mut request = client.get(format!("{base}{path}?{encoded}"));
            if let Some(tenant) = tenant {
                request = request.header("x-scope-orgid", tenant);
            }
            let response = request.send().await?;
            let status = response.status();
            if status.is_success() {
                let value = response.json().await?;
                if !require_non_empty || flame_names(&value).len() > 1 {
                    return Ok(value);
                }
                attempts.push(format!("{path} query={query}: {status}: empty flamegraph"));
                continue;
            }
            let body = response.text().await.unwrap_or_default();
            attempts.push(format!("{path} query={query}: {status}: {body}"));
        }
    }
    Err(format!(
        "no render endpoint succeeded for {base}: {}",
        attempts.join(" | ")
    )
    .into())
}

async fn render_until_non_empty(
    client: &reqwest::Client,
    base: &str,
    queries: &[String],
    from: &str,
    until: &str,
    tenant: Option<&str>,
) -> TestResult<Value> {
    let mut last = None;
    for _ in 0..90 {
        let value = render_any(client, base, queries, from, until, tenant, true).await;
        match value {
            Ok(value) => return Ok(value),
            Err(err) => last = Some(err.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Err(last
        .unwrap_or_else(|| "render did not become non-empty".to_string())
        .into())
}

async fn assert_profile_types_contain(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let response = connect_json_until(
        client,
        base,
        tenant,
        "ProfileTypes",
        json_time_range(),
        |value| {
            value
                .get("profileTypes")
                .or_else(|| value.get("profile_types"))
                .and_then(Value::as_array)
                .is_some_and(|types| !types.is_empty())
        },
    )
    .await?;
    let ids = response
        .get("profileTypes")
        .or_else(|| response.get("profile_types"))
        .and_then(Value::as_array)
        .ok_or_else(|| format!("ProfileTypes response missing profileTypes: {response}"))?
        .iter()
        .inspect(|profile_type| {
            if profile_type
                .get("ID")
                .or_else(|| profile_type.get("id"))
                .and_then(Value::as_str)
                == Some(PROFILE_TYPE)
            {
                let field_cases = [
                    ("name", "goroutines"),
                    ("sampleType", "goroutine"),
                    ("sampleUnit", "count"),
                    ("periodType", "goroutine"),
                    ("periodUnit", "count"),
                ];
                for (field, expected) in field_cases {
                    assert_eq!(
                        profile_type.get(field).and_then(Value::as_str),
                        Some(expected),
                        "profile type field `{field}`"
                    );
                }
            }
        })
        .filter_map(|value| {
            value
                .get("ID")
                .or_else(|| value.get("id"))
                .and_then(Value::as_str)
        })
        .collect::<BTreeSet<_>>();
    if !ids.contains(PROFILE_TYPE) {
        return Err(format!("ProfileTypes did not include {PROFILE_TYPE}: {response}").into());
    }
    Ok(())
}

async fn assert_label_names_contain(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let response = connect_json(client, base, tenant, "LabelNames", json_time_range()).await?;
    let names = string_array(&response, "names")?;
    for expected in ["__name__", "__profile_type__", "env"] {
        if !names.contains(expected) {
            return Err(format!("LabelNames did not include {expected}: {response}").into());
        }
    }
    Ok(())
}

async fn assert_label_values_contain(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    name: &str,
    expected: &str,
) -> TestResult {
    let response = connect_json(
        client,
        base,
        tenant,
        "LabelValues",
        json!({
            "name": name,
            "start": query_start_ms(),
            "end": query_end_ms(),
        }),
    )
    .await?;
    let values = string_array(&response, "names")?;
    if !values.contains(expected) {
        return Err(format!("LabelValues({name}) did not include {expected}: {response}").into());
    }
    Ok(())
}

async fn assert_label_names_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    krabka_base: &str,
) -> TestResult {
    let body = json!({
        "matchers": [SELECTOR],
        "start": query_start_ms(),
        "end": query_end_ms(),
    });
    let pyroscope = connect_json(client, pyroscope_base, None, "LabelNames", body.clone()).await?;
    let krabka = connect_json(
        client,
        krabka_base,
        Some(TENANT),
        "LabelNames",
        body.clone(),
    )
    .await?;

    assert_label_names_equal(&pyroscope, &krabka)
}

async fn assert_profile_types_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    krabka_base: &str,
) -> TestResult {
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "ProfileTypes",
        json_time_range(),
        |value| canonical_profile_type(value, PROFILE_TYPE).is_ok(),
    )
    .await?;
    let krabka = connect_json_until(
        client,
        krabka_base,
        Some(TENANT),
        "ProfileTypes",
        json_time_range(),
        |value| canonical_profile_type(value, PROFILE_TYPE).is_ok(),
    )
    .await?;

    assert_canonical_json_equal(
        "ProfileTypes",
        canonical_profile_type(&pyroscope, PROFILE_TYPE)?,
        canonical_profile_type(&krabka, PROFILE_TYPE)?,
    )
}

async fn assert_label_values_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    krabka_base: &str,
    name: &str,
) -> TestResult {
    let body = json!({
        "name": name,
        "start": query_start_ms(),
        "end": query_end_ms(),
    });
    let pyroscope = connect_json(client, pyroscope_base, None, "LabelValues", body.clone()).await?;
    let krabka = connect_json(
        client,
        krabka_base,
        Some(TENANT),
        "LabelValues",
        body.clone(),
    )
    .await?;

    assert_canonical_json_equal(
        &format!("LabelValues({name})"),
        canonical_string_list(&pyroscope, "names")?,
        canonical_string_list(&krabka, "names")?,
    )
}

async fn assert_select_series_has_points(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let response = connect_json(
        client,
        base,
        tenant,
        "SelectSeries",
        json!({
            "profileTypeID": PROFILE_TYPE,
            "labelSelector": SELECTOR,
            "start": query_start_ms(),
            "end": query_end_ms(),
            "groupBy": ["env"],
            "step": 10.0,
            "aggregation": "TIME_SERIES_AGGREGATION_TYPE_SUM",
            "limit": 10,
        }),
    )
    .await?;
    let series = response
        .get("series")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("SelectSeries response missing series: {response}"))?;
    let has_point = series.iter().any(|series| {
        series
            .get("points")
            .and_then(Value::as_array)
            .is_some_and(|points| points.iter().any(|point| point_value(point) > 0.0))
    });
    if !has_point {
        return Err(format!("SelectSeries had no positive points: {response}").into());
    }
    Ok(())
}

async fn assert_select_series_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    krabka_base: &str,
    fixture_timestamp: i64,
) -> TestResult {
    let body = select_series_body(fixture_timestamp);
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "SelectSeries",
        body.clone(),
        select_series_has_positive_point,
    )
    .await?;
    let krabka = connect_json_until(
        client,
        krabka_base,
        Some(TENANT),
        "SelectSeries",
        body,
        select_series_has_positive_point,
    )
    .await?;

    assert_select_series_equal(&pyroscope, &krabka)
}

async fn assert_select_heatmap_has_slots(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let response = connect_json(
        client,
        base,
        tenant,
        "SelectHeatmap",
        json!({
            "profileTypeID": PROFILE_TYPE,
            "labelSelector": SELECTOR,
            "start": query_start_ms(),
            "end": query_end_ms(),
            "step": 10.0,
            "groupBy": ["env"],
            "queryType": "HEATMAP_QUERY_TYPE_INDIVIDUAL",
            "exemplarType": "EXEMPLAR_TYPE_NONE",
            "limit": 10,
        }),
    )
    .await?;
    let series = response
        .get("series")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("SelectHeatmap response missing series: {response}"))?;
    let has_slot = series.iter().any(|series| {
        series
            .get("slots")
            .and_then(Value::as_array)
            .is_some_and(|slots| !slots.is_empty())
    });
    if !has_slot {
        return Err(format!("SelectHeatmap had no slots: {response}").into());
    }
    Ok(())
}

async fn assert_select_merge_stacktraces_has_symbol(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let response = connect_json(
        client,
        base,
        tenant,
        "SelectMergeStacktraces",
        json!({
            "profileTypeID": PROFILE_TYPE,
            "labelSelector": SELECTOR,
            "start": query_start_ms(),
            "end": query_end_ms(),
            "maxNodes": 1024,
            "format": "PROFILE_FORMAT_FLAMEGRAPH",
        }),
    )
    .await?;
    let flamegraph = response
        .get("flamegraph")
        .ok_or_else(|| format!("SelectMergeStacktraces response missing flamegraph: {response}"))?;
    if flamegraph_ticks(flamegraph) <= 0 {
        return Err(format!("SelectMergeStacktraces had no positive ticks: {response}").into());
    }
    let names = flamegraph_names(flamegraph);
    if !names.contains("runtime/pprof.profileWriter") {
        return Err(format!(
            "SelectMergeStacktraces missed runtime/pprof.profileWriter: {response}"
        )
        .into());
    }
    Ok(())
}

async fn assert_select_merge_stacktraces_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    krabka_base: &str,
    fixture_timestamp: i64,
) -> TestResult {
    let body = select_merge_stacktraces_body(fixture_timestamp);
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "SelectMergeStacktraces",
        body.clone(),
        |value| {
            value
                .get("flamegraph")
                .is_some_and(|flamegraph| flamegraph_ticks(flamegraph) > 0)
        },
    )
    .await?;
    let krabka = connect_json_until(
        client,
        krabka_base,
        Some(TENANT),
        "SelectMergeStacktraces",
        body,
        |value| {
            value
                .get("flamegraph")
                .is_some_and(|flamegraph| flamegraph_ticks(flamegraph) > 0)
        },
    )
    .await?;

    assert_connect_flamegraph_equal("SelectMergeStacktraces", &pyroscope, &krabka)
}

async fn assert_select_merge_pprof_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    krabka_base: &str,
    fixture_timestamp: i64,
) -> TestResult {
    let mut body = select_merge_stacktraces_body(fixture_timestamp);
    body["format"] = json!("PROFILE_FORMAT_PPROF");
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "SelectMergeStacktraces",
        body.clone(),
        |value| pprof_total(value) > 0,
    )
    .await?;
    let krabka = connect_json_until(
        client,
        krabka_base,
        Some(TENANT),
        "SelectMergeStacktraces",
        body,
        |value| pprof_total(value) > 0,
    )
    .await?;

    assert_eq!(pprof_total(&pyroscope), pprof_total(&krabka));
    assert_eq!(
        pprof_function_names(&pyroscope),
        pprof_function_names(&krabka)
    );
    Ok(())
}

fn pprof_total(response: &Value) -> i64 {
    response
        .pointer("/pprof/profile/sample")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|sample| sample.get("value").and_then(Value::as_array))
        .flatten()
        .filter_map(json_i64)
        .sum()
}

fn pprof_function_names(response: &Value) -> BTreeSet<&str> {
    let Some(profile) = response.pointer("/pprof/profile") else {
        return BTreeSet::new();
    };
    let Some(strings) = profile.get("stringTable").and_then(Value::as_array) else {
        return BTreeSet::new();
    };
    profile
        .get("function")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|function| function.get("name").and_then(json_i64))
        .filter_map(|index| usize::try_from(index).ok())
        .filter_map(|index| strings.get(index).and_then(Value::as_str))
        .collect()
}

async fn assert_async_request_contract(
    client: &reqwest::Client,
    pyroscope_base: &str,
    krabka_base: &str,
    fixture_timestamp: i64,
) -> TestResult {
    let mut body = select_merge_stacktraces_body(fixture_timestamp);
    body["async"] = json!({ "type": "ASYNC_QUERY_TYPE_FORCE" });
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "SelectMergeStacktraces",
        body.clone(),
        |value| {
            value
                .get("flamegraph")
                .is_some_and(|flamegraph| flamegraph_ticks(flamegraph) > 0)
        },
    )
    .await?;
    assert!(
        pyroscope
            .get("flamegraph")
            .is_some_and(|flamegraph| flamegraph_ticks(flamegraph) > 0)
    );
    let actual = connect_json(
        client,
        krabka_base,
        Some(TENANT),
        "SelectMergeStacktraces",
        body,
    )
    .await?;
    assert!(actual.get("async").is_none());
    let expected: pb::querier::v1::FlameGraph =
        serde_json::from_value(pyroscope["flamegraph"].clone())?;
    let actual: pb::querier::v1::FlameGraph = serde_json::from_value(actual["flamegraph"].clone())?;
    assert_eq!(
        normalized_flamegraph_stacks(&actual)?,
        normalized_flamegraph_stacks(&expected)?
    );
    assert_eq!(actual.total, expected.total);

    Ok(())
}

async fn assert_diff_has_ticks(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
) -> TestResult {
    let query = json!({
        "profileTypeID": PROFILE_TYPE,
        "labelSelector": SELECTOR,
        "start": query_start_ms(),
        "end": query_end_ms(),
        "maxNodes": 1024,
        "format": "PROFILE_FORMAT_FLAMEGRAPH",
    });
    let response = connect_json(
        client,
        base,
        tenant,
        "Diff",
        json!({
            "left": query,
            "right": query,
        }),
    )
    .await?;
    let flamegraph = response
        .get("flamegraph")
        .ok_or_else(|| format!("Diff response missing flamegraph: {response}"))?;
    let left_ticks = flamegraph
        .get("leftTicks")
        .or_else(|| flamegraph.get("left_ticks"))
        .and_then(json_i64)
        .unwrap_or_default();
    let right_ticks = flamegraph
        .get("rightTicks")
        .or_else(|| flamegraph.get("right_ticks"))
        .and_then(json_i64)
        .unwrap_or_default();
    if left_ticks <= 0 || right_ticks <= 0 {
        return Err(format!("Diff had no positive side ticks: {response}").into());
    }
    Ok(())
}

async fn assert_diff_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    krabka_base: &str,
    fixture_timestamp: i64,
) -> TestResult {
    let body = diff_body(fixture_timestamp);
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "Diff",
        body.clone(),
        diff_has_positive_ticks,
    )
    .await?;
    let krabka = connect_json_until(
        client,
        krabka_base,
        Some(TENANT),
        "Diff",
        body,
        diff_has_positive_ticks,
    )
    .await?;

    assert_diff_equal(&pyroscope, &krabka)
}

fn select_merge_stacktraces_body(fixture_timestamp: i64) -> Value {
    json!({
        "profileTypeID": PROFILE_TYPE,
        "labelSelector": SELECTOR,
        "start": fixture_timestamp - 20_000,
        "end": fixture_timestamp + 20_000,
        "maxNodes": 1024,
        "format": "PROFILE_FORMAT_FLAMEGRAPH",
    })
}

fn diff_body(fixture_timestamp: i64) -> Value {
    let query = select_merge_stacktraces_body(fixture_timestamp);
    json!({
        "left": query,
        "right": query,
    })
}

fn select_series_body(fixture_timestamp: i64) -> Value {
    // An explicit range prevents independent upstream request sanitization.
    // The fractional-second start preserves evidence of bucket alignment.
    json!({
        "profileTypeID": PROFILE_TYPE,
        "labelSelector": SELECTOR,
        "start": fixture_timestamp - 20_000 + 123,
        "end": fixture_timestamp + 20_000,
        "groupBy": ["env"],
        "step": 10.0,
        "aggregation": "TIME_SERIES_AGGREGATION_TYPE_SUM",
        "limit": 10,
    })
}

async fn connect_json_until(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    method: &str,
    body: Value,
    ready: impl Fn(&Value) -> bool,
) -> TestResult<Value> {
    let mut last = None;
    for _ in 0..90 {
        let value = connect_json(client, base, tenant, method, body.clone()).await;
        match value {
            Ok(value) if ready(&value) => return Ok(value),
            Ok(value) => last = Some(format!("{method} response not ready for {body}: {value}")),
            Err(err) => last = Some(err.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Err(last
        .unwrap_or_else(|| format!("{method} response did not become ready"))
        .into())
}

async fn connect_json(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    method: &str,
    body: Value,
) -> TestResult<Value> {
    let mut request = client
        .post(format!("{base}/querier.v1.QuerierService/{method}"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(&body);
    if let Some(tenant) = tenant {
        request = request.header("x-scope-orgid", tenant);
    }
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(format!("{method} returned {status}: {text}").into());
    }
    serde_json::from_str(&text)
        .map_err(|err| format!("{method} returned non-JSON body `{text}`: {err}").into())
}

fn string_array<'a>(value: &'a Value, key: &str) -> TestResult<BTreeSet<&'a str>> {
    Ok(value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("response missing {key} array: {value}"))?
        .iter()
        .filter_map(Value::as_str)
        .collect())
}

fn select_series_has_positive_point(value: &Value) -> bool {
    value
        .get("series")
        .and_then(Value::as_array)
        .is_some_and(|series| {
            series.iter().any(|series| {
                series
                    .get("points")
                    .and_then(Value::as_array)
                    .is_some_and(|points| points.iter().any(|point| point_value(point) > 0.0))
            })
        })
}

fn diff_has_positive_ticks(value: &Value) -> bool {
    value.get("flamegraph").is_some_and(|flamegraph| {
        flamegraph
            .get("leftTicks")
            .or_else(|| flamegraph.get("left_ticks"))
            .and_then(json_i64)
            .unwrap_or_default()
            > 0
            && flamegraph
                .get("rightTicks")
                .or_else(|| flamegraph.get("right_ticks"))
                .and_then(json_i64)
                .unwrap_or_default()
                > 0
    })
}

fn canonical_profile_type(value: &Value, expected_id: &str) -> TestResult<Value> {
    let profile_types = value
        .get("profileTypes")
        .or_else(|| value.get("profile_types"))
        .and_then(Value::as_array)
        .ok_or_else(|| format!("ProfileTypes response missing profileTypes: {value}"))?;
    let profile_type = profile_types
        .iter()
        .find(|profile_type| {
            profile_type
                .get("ID")
                .or_else(|| profile_type.get("id"))
                .and_then(Value::as_str)
                == Some(expected_id)
        })
        .ok_or_else(|| format!("ProfileTypes response missing {expected_id}: {value}"))?;

    Ok(json!({
        "id": profile_type
            .get("ID")
            .or_else(|| profile_type.get("id"))
            .and_then(Value::as_str),
        "name": profile_type.get("name").and_then(Value::as_str),
        "sampleType": profile_type.get("sampleType").and_then(Value::as_str),
        "sampleUnit": profile_type.get("sampleUnit").and_then(Value::as_str),
        "periodType": profile_type.get("periodType").and_then(Value::as_str),
        "periodUnit": profile_type.get("periodUnit").and_then(Value::as_str),
    }))
}

fn canonical_string_list(value: &Value, key: &str) -> TestResult<Value> {
    Ok(json!({
        key: string_array(value, key)?.into_iter().collect::<Vec<_>>()
    }))
}

fn point_value(point: &Value) -> f64 {
    point
        .get("value")
        .and_then(Value::as_f64)
        .or_else(|| point.get("value").and_then(Value::as_str)?.parse().ok())
        .unwrap_or_default()
}

fn flamegraph_names(value: &Value) -> BTreeSet<String> {
    value
        .get("names")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|names| names.iter())
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn flamegraph_ticks(value: &Value) -> i64 {
    value
        .get("total")
        .or_else(|| value.get("leftTicks"))
        .or_else(|| value.get("left_ticks"))
        .and_then(json_i64)
        .unwrap_or_default()
}

fn json_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

/// The base URL of `pyroscope`'s HTTP port, once its `/ready` answers.
async fn ready_pyroscope_base(
    client: &reqwest::Client,
    pyroscope: &testcontainers::ContainerAsync<GenericImage>,
) -> TestResult<String> {
    let base = mapped_base_url(pyroscope, PYROSCOPE_HTTP_PORT).await?;
    wait_for_http_ok(client, &base, &["/ready"]).await?;
    Ok(base)
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, paths: &[&str]) -> TestResult {
    for _ in 0..300 {
        for path in paths {
            if let Ok(response) = client.get(format!("{base}{path}")).send().await
                && response.status().is_success()
            {
                return Ok(());
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Err(format!("{base} did not become ready").into())
}

fn flame_names(value: &Value) -> BTreeSet<String> {
    value
        .pointer("/flamebearer/names")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|names| names.iter())
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn flame_ticks(value: &Value) -> Option<i64> {
    value
        .pointer("/flamebearer/numTicks")
        .or_else(|| value.pointer("/flamebearer/total"))
        .and_then(Value::as_i64)
}

fn assert_flamebearer_equal(expected: &Value, actual: &Value) -> TestResult {
    let expected = canonical_flamebearer(expected)?;
    let actual = canonical_flamebearer(actual)?;
    if expected != actual {
        return Err(format!(
            "flamebearer mismatch:\nexpected summary:\n{}\nactual summary:\n{}\nexpected {expected}\ngot {actual}",
            flamebearer_summary(&expected),
            flamebearer_summary(&actual),
        )
        .into());
    }
    Ok(())
}

fn flamebearer_summary(value: &Value) -> String {
    let names = value
        .get("names")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let levels = value
        .get("levels")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for (level_idx, level) in levels.iter().take(5).enumerate() {
        let Some(values) = level.as_array() else {
            continue;
        };
        let mut x = 0_i64;
        let mut bars = Vec::new();
        for chunk in values.chunks(4).take(8) {
            let [delta, total, self_, name_idx] = chunk else {
                continue;
            };
            x += json_i64(delta).unwrap_or_default();
            let total = json_i64(total).unwrap_or_default();
            let self_ = json_i64(self_).unwrap_or_default();
            let name_idx = json_i64(name_idx).unwrap_or_default();
            let name = usize::try_from(name_idx)
                .ok()
                .and_then(|idx| names.get(idx))
                .and_then(Value::as_str)
                .unwrap_or("<missing>");
            bars.push(format!("{name}@{x}+{total}/self={self_}"));
            x += total;
        }
        out.push(format!("L{level_idx}: {}", bars.join(" | ")));
    }
    out.join("\n")
}

fn assert_canonical_json_equal(method: &str, expected: Value, actual: Value) -> TestResult {
    let expected = canonical_json(expected);
    let actual = canonical_json(actual);
    if expected != actual {
        return Err(format!("{method} mismatch: expected {expected}, got {actual}").into());
    }
    Ok(())
}

fn assert_label_names_equal(expected: &Value, actual: &Value) -> TestResult {
    assert_canonical_json_equal(
        "LabelNames",
        canonical_string_list(expected, "names")?,
        canonical_string_list(actual, "names")?,
    )
}

fn assert_connect_flamegraph_equal(method: &str, expected: &Value, actual: &Value) -> TestResult {
    let expected = canonical_connect_flamegraph(expected)?;
    let actual = canonical_connect_flamegraph(actual)?;
    // Names and bar values are indexed arrays: independently sorting them
    // loses the association between a stack and its sample value.
    if expected != actual {
        return Err(format!("{method} mismatch: expected {expected}, got {actual}").into());
    }
    Ok(())
}

fn assert_select_series_equal(expected: &Value, actual: &Value) -> TestResult {
    assert_canonical_json_equal(
        "SelectSeries",
        canonical_select_series(expected)?,
        canonical_select_series(actual)?,
    )
}

fn assert_diff_equal(expected: &Value, actual: &Value) -> TestResult {
    let expected = canonical_diff(expected)?;
    let actual = canonical_diff(actual)?;
    // Resolve indices and layout coordinates before sorting complete stack
    // records. Independently sorting names or values loses their association.
    if expected != actual {
        return Err(format!("Diff mismatch: expected {expected}, got {actual}").into());
    }
    Ok(())
}

fn canonical_diff(value: &Value) -> TestResult<Value> {
    let flamegraph = value
        .get("flamegraph")
        .ok_or_else(|| format!("Diff response missing flamegraph object: {value}"))?;
    let names = flamegraph
        .get("names")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("Diff flamegraph missing names array: {value}"))?
        .iter()
        .map(|name| name.as_str().ok_or("Diff name is not a string".into()))
        .collect::<TestResult<Vec<_>>>()?;
    let levels = flamegraph
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("Diff flamegraph missing levels array: {value}"))?
        .iter()
        .map(|level| {
            let values = level
                .get("values")
                .and_then(Value::as_array)
                .ok_or("Diff level missing values")?
                .iter()
                .map(|value| json_i64(value).ok_or("Diff bar value is not an integer".into()))
                .collect::<TestResult<Vec<_>>>()?;
            let (bars, remainder) = values.as_chunks::<7>();
            if !remainder.is_empty()
                || bars.iter().any(|bar| {
                    usize::try_from(bar[6])
                        .ok()
                        .and_then(|index| names.get(index))
                        .is_none()
                })
            {
                return Err("Diff bar is malformed or references an invalid name".into());
            }
            Ok(values)
        })
        .collect::<TestResult<Vec<_>>>()?;
    let bars = normalized_diff_bars(&names, &levels)?;
    let total = flamegraph
        .get("total")
        .and_then(json_i64)
        .ok_or_else(|| format!("Diff flamegraph missing total: {value}"))?;
    let max_self = flamegraph
        .get("maxSelf")
        .or_else(|| flamegraph.get("max_self"))
        .and_then(json_i64)
        .ok_or_else(|| format!("Diff flamegraph missing maxSelf: {value}"))?;
    let left_ticks = flamegraph
        .get("leftTicks")
        .or_else(|| flamegraph.get("left_ticks"))
        .and_then(json_i64)
        .ok_or_else(|| format!("Diff flamegraph missing leftTicks: {value}"))?;
    let right_ticks = flamegraph
        .get("rightTicks")
        .or_else(|| flamegraph.get("right_ticks"))
        .and_then(json_i64)
        .ok_or_else(|| format!("Diff flamegraph missing rightTicks: {value}"))?;

    Ok(json!({
        "bars": bars,
        "total": total,
        "maxSelf": max_self,
        "leftTicks": left_ticks,
        "rightTicks": right_ticks,
    }))
}

/// Layout and name-table order are serialization choices. Resolve both side's
/// delta coordinates to parent rectangles, then preserve every stack's paired
/// totals/self values and duplicate entries in a sorted semantic record list.
fn normalized_diff_bars(
    names: &[&str],
    levels: &[Vec<i64>],
) -> TestResult<Vec<(Vec<String>, [i64; 4])>> {
    let mut parents: Vec<(i64, i64, i64, i64, Vec<String>)> = Vec::new();
    let mut records = Vec::new();
    for (depth, values) in levels.iter().enumerate() {
        let (bars, remainder) = values.as_chunks::<7>();
        if !remainder.is_empty() || (depth == 0 && bars.len() != 1) {
            return Err("Diff has malformed bars or no unique root".into());
        }
        let mut current = Vec::new();
        let mut previous_left_end = 0_i64;
        let mut previous_right_end = 0_i64;
        for bar in bars {
            let name = usize::try_from(bar[6])
                .ok()
                .and_then(|index| names.get(index))
                .ok_or("Diff name index out of bounds")?;
            if bar[1] < 0
                || bar[2] < 0
                || bar[2] > bar[1]
                || bar[4] < 0
                || bar[5] < 0
                || bar[5] > bar[4]
            {
                return Err("Diff has a negative width/self value or self exceeds total".into());
            }
            let left = previous_left_end
                .checked_add(bar[0])
                .ok_or("Diff left offset overflow")?;
            let left_end = left
                .checked_add(bar[1])
                .ok_or("Diff left extent overflow")?;
            let right = previous_right_end
                .checked_add(bar[3])
                .ok_or("Diff right offset overflow")?;
            let right_end = right
                .checked_add(bar[4])
                .ok_or("Diff right extent overflow")?;
            if left < 0 || right < 0 {
                return Err("Diff has a negative absolute offset".into());
            }
            let mut path = if depth == 0 {
                if left != 0 || right != 0 {
                    return Err("Diff root does not start at zero".into());
                }
                Vec::new()
            } else {
                let mut candidates = parents.iter().filter(|(l, le, r, re, _)| {
                    *l <= left && left_end <= *le && *r <= right && right_end <= *re
                });
                let parent = candidates
                    .next()
                    .ok_or("Diff bar has no containing parent")?;
                if candidates.next().is_some() {
                    return Err("Diff bar has ambiguous containing parents".into());
                }
                parent.4.clone()
            };
            path.push((*name).to_string());
            records.push((path.clone(), [bar[1], bar[2], bar[4], bar[5]]));
            current.push((left, left_end, right, right_end, path));
            previous_left_end = left_end;
            previous_right_end = right_end;
        }
        parents = current;
    }
    records.sort();
    Ok(records)
}

fn canonical_select_series(value: &Value) -> TestResult<Value> {
    let series = value
        .get("series")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("SelectSeries response missing series array: {value}"))?;
    let canonical = series
        .iter()
        .map(|series| {
            let labels = series
                .get("labels")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("SelectSeries series missing labels array: {value}"))?
                .iter()
                .map(|label| {
                    Ok(json!({
                        "name": label.get("name").and_then(Value::as_str),
                        "value": label.get("value").and_then(Value::as_str),
                    }))
                })
                .collect::<TestResult<Vec<_>>>()?;
            let points = series
                .get("points")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("SelectSeries series missing points array: {value}"))?
                .iter()
                .map(|point| {
                    let timestamp = point
                        .get("timestamp")
                        .and_then(json_i64)
                        .ok_or_else(|| format!("SelectSeries point missing timestamp: {value}"))?;
                    Ok(json!({
                        "timestamp": timestamp,
                        "value": point_value(point),
                    }))
                })
                .collect::<TestResult<Vec<_>>>()?;
            Ok(json!({
                "labels": labels,
                "points": points,
            }))
        })
        .collect::<TestResult<Vec<_>>>()?;

    Ok(json!({ "series": canonical }))
}

fn canonical_connect_flamegraph(value: &Value) -> TestResult<Value> {
    let flamegraph = value
        .get("flamegraph")
        .ok_or_else(|| format!("response missing flamegraph object: {value}"))?;
    let names = flamegraph
        .get("names")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("flamegraph missing names array: {value}"))?;
    let level_values = flamegraph
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("flamegraph missing levels array: {value}"))?;
    let mut levels = Vec::with_capacity(level_values.len());
    for level in level_values {
        levels.push(
            level
                .get("values")
                .and_then(Value::as_array)
                .cloned()
                .ok_or_else(|| format!("flamegraph level missing values array: {value}"))?,
        );
    }
    let total = flamegraph
        .get("total")
        .and_then(json_i64)
        .ok_or_else(|| format!("flamegraph missing total: {value}"))?;
    let max_self = flamegraph
        .get("maxSelf")
        .or_else(|| flamegraph.get("max_self"))
        .and_then(json_i64)
        .ok_or_else(|| format!("flamegraph missing maxSelf: {value}"))?;

    Ok(json!({
        "names": names,
        "levels": levels,
        "total": total,
        "maxSelf": max_self,
    }))
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Array(values)
            if values
                .iter()
                .all(|value| value.as_str().is_some() || value.as_i64().is_some()) =>
        {
            let mut values = values.into_iter().map(canonical_json).collect::<Vec<_>>();
            values.sort_by_key(ToString::to_string);
            Value::Array(values)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        other => other,
    }
}

fn canonical_flamebearer(value: &Value) -> TestResult<Value> {
    let flamebearer = value
        .get("flamebearer")
        .ok_or_else(|| format!("response missing flamebearer object: {value}"))?;
    let names = flamebearer
        .get("names")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("flamebearer missing names array: {value}"))?;
    let levels = flamebearer
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("flamebearer missing levels array: {value}"))?;
    let ticks = flamebearer
        .get("numTicks")
        .or_else(|| flamebearer.get("total"))
        .and_then(json_i64)
        .ok_or_else(|| format!("flamebearer missing numTicks/total: {value}"))?;
    let max_self = flamebearer
        .get("maxSelf")
        .or_else(|| flamebearer.get("max_self"))
        .and_then(json_i64)
        .ok_or_else(|| format!("flamebearer missing maxSelf: {value}"))?;

    Ok(json!({
        "names": names,
        "levels": levels,
        "numTicks": ticks,
        "maxSelf": max_self,
    }))
}

fn query_end_ms() -> i64 {
    i64::MAX
}

fn query_start_ms() -> i64 {
    0
}

fn json_time_range() -> Value {
    json!({ "start": query_start_ms(), "end": query_end_ms() })
}

#[test]
fn flamebearer_differential_rejects_shape_drift() {
    let expected = json!({
        "flamebearer": {
            "names": ["total", "main"],
            "levels": [[0, 7, 0, 0], [0, 7, 7, 1]],
            "numTicks": 7,
            "maxSelf": 7
        }
    });
    let actual = json!({
        "flamebearer": {
            "names": ["total", "main"],
            "levels": [[0, 8, 0, 0], [0, 8, 8, 1]],
            "numTicks": 8,
            "maxSelf": 8
        }
    });

    let err = assert_flamebearer_equal(&expected, &actual).unwrap_err();
    assert!(err.to_string().contains("flamebearer mismatch"));
}

#[test]
fn connect_differential_rejects_canonical_response_drift() {
    let expected = json!({ "names": ["__name__", "env"] });
    let actual = json!({ "names": ["__name__", "service_name"] });

    let err = assert_canonical_json_equal("LabelNames", expected, actual).unwrap_err();
    assert!(err.to_string().contains("LabelNames mismatch"));
}

#[test]
fn label_names_differential_rejects_name_drift() {
    let expected = json!({ "names": ["__name__", "env"] });
    let actual = json!({ "names": ["__name__", "service_name"] });

    let err = assert_label_names_equal(&expected, &actual).unwrap_err();
    assert!(err.to_string().contains("LabelNames mismatch"));
}

#[test]
fn connect_flamegraph_differential_rejects_tick_drift() {
    let expected = json!({
        "flamegraph": {
            "names": ["total", "main"],
            "levels": [{ "values": [0, 7, 0, 0] }, { "values": [0, 7, 7, 1] }],
            "total": 7,
            "maxSelf": 7
        }
    });
    let actual = json!({
        "flamegraph": {
            "names": ["total", "main"],
            "levels": [{ "values": [0, 8, 0, 0] }, { "values": [0, 8, 8, 1] }],
            "total": 8,
            "maxSelf": 8
        }
    });

    let err =
        assert_connect_flamegraph_equal("SelectMergeStacktraces", &expected, &actual).unwrap_err();
    assert!(err.to_string().contains("SelectMergeStacktraces mismatch"));
}

#[test]
fn connect_series_differential_rejects_point_drift() {
    let expected = json!({
        "series": [{
            "labels": [{ "name": "env", "value": PROFILE_ENV }],
            "points": [{ "timestamp": 10, "value": 7.0 }]
        }]
    });
    let actual = json!({
        "series": [{
            "labels": [{ "name": "env", "value": PROFILE_ENV }],
            "points": [{ "timestamp": 10, "value": 8.0 }]
        }]
    });

    let err = assert_select_series_equal(&expected, &actual).unwrap_err();
    assert!(err.to_string().contains("SelectSeries mismatch"));
}

fn shared_api_tuple() -> Vec<(String, String)> {
    vec![
        ("service_name".to_string(), "api".to_string()),
        ("__profile_type__".to_string(), PROFILE_TYPE.to_string()),
    ]
}

fn projected_set(value: &str, profile_type: &str) -> Value {
    json!({ "labels": [
        { "name": "service_name", "value": value },
        { "name": "__profile_type__", "value": profile_type }
    ] })
}

#[test]
fn series_differential_rejects_wire_key_casing_drift() {
    let tuple = shared_api_tuple();
    let camel = json!({ "labelsSet": [projected_set("api", PROFILE_TYPE)] });
    let snake = json!({ "labels_set": [projected_set("api", PROFILE_TYPE)] });

    let err = assert_series_drilldown_compatible(&camel, &snake, &tuple).unwrap_err();
    assert!(err.to_string().contains("wire key differs"), "{err}");
}

#[test]
fn series_differential_rejects_spurious_empty_label_set() {
    // The defining symptom of the in-memory `series()` bug: a `{}` entry inserted
    // when `labelNames` is empty. The krabka (second) argument carries it.
    let tuple = shared_api_tuple();
    let pyroscope = json!({ "labelsSet": [projected_set("api", PROFILE_TYPE)] });
    let krabka = json!({
        "labelsSet": [
            { "labels": [] },
            projected_set("api", PROFILE_TYPE)
        ]
    });

    let err = assert_series_drilldown_compatible(&pyroscope, &krabka, &tuple).unwrap_err();
    assert!(
        err.to_string().contains("spurious empty label set"),
        "{err}"
    );
}

#[test]
fn series_differential_rejects_missing_shared_tuple_on_krabka() {
    // The core drilldown regression: krabka returns label sets, but NOT the
    // ingested `api` series (e.g. it time-scoped a no-range request to [0,0] and
    // dropped everything but some unrelated series), so the shared tuple is
    // absent and the grid shows "No data".
    let tuple = shared_api_tuple();
    let pyroscope = json!({ "labelsSet": [projected_set("api", PROFILE_TYPE)] });
    let krabka = json!({ "labelsSet": [projected_set("other", PROFILE_TYPE)] });

    let err = assert_series_drilldown_compatible(&pyroscope, &krabka, &tuple).unwrap_err();
    assert!(
        err.to_string().contains("krabka missing shared tuple"),
        "{err}"
    );
}

#[test]
fn series_differential_rejects_empty_krabka_response() {
    // A literal `{}` from krabka (no `labelsSet` at all) is the exact shape the
    // no-range drilldown call elicits today; it fails the wire-key check.
    let tuple = shared_api_tuple();
    let pyroscope = json!({ "labelsSet": [projected_set("api", PROFILE_TYPE)] });
    let krabka = json!({});

    let err = assert_series_drilldown_compatible(&pyroscope, &krabka, &tuple).unwrap_err();
    assert!(err.to_string().contains("wire key differs"), "{err}");
}

#[test]
fn series_differential_rejects_intra_set_key_reordering() {
    let tuple = shared_api_tuple();
    let pyroscope = json!({ "labelsSet": [{ "labels": [
        { "name": "service_name", "value": "api" },
        { "name": "__profile_type__", "value": PROFILE_TYPE }
    ] }] });
    let krabka = json!({ "labelsSet": [{ "labels": [
        { "name": "__profile_type__", "value": PROFILE_TYPE },
        { "name": "service_name", "value": "api" }
    ] }] });

    let err = assert_series_drilldown_compatible(&pyroscope, &krabka, &tuple).unwrap_err();
    assert!(
        err.to_string()
            .contains("key order for shared tuple differs"),
        "{err}"
    );
}

#[test]
fn series_differential_accepts_pyroscope_superset() {
    // Real Pyroscope's enumeration is a strict superset (it self-instruments);
    // extra `pyroscope` series on the Pyroscope side must NOT be a difference, as
    // long as the shared `api` tuple is present on both with the same key order.
    let tuple = shared_api_tuple();
    let pyroscope = json!({ "labelsSet": [
        projected_set("pyroscope", "process_cpu:cpu:nanoseconds:cpu:nanoseconds"),
        projected_set("api", PROFILE_TYPE)
    ] });
    let krabka = json!({ "labelsSet": [projected_set("api", PROFILE_TYPE)] });

    assert_series_drilldown_compatible(&pyroscope, &krabka, &tuple).unwrap();
}

#[test]
fn series_full_differential_rejects_spurious_empty_label_set() {
    let pyroscope = json!({ "labelsSet": [{ "labels": [
        { "name": "__name__", "value": "goroutines" },
        { "name": "__profile_type__", "value": PROFILE_TYPE },
        { "name": "env", "value": PROFILE_ENV },
        { "name": "service_name", "value": "api" }
    ] }] });
    let krabka = json!({ "labelsSet": [{ "labels": [] }] });

    let err = assert_series_full_compatible(&pyroscope, &krabka).unwrap_err();
    assert!(
        err.to_string().contains("spurious empty label set"),
        "{err}"
    );
}

#[test]
fn profile_stats_differential_rejects_int_representation_drift() {
    // Canonical proto-JSON encodes int64 as a string; a number-typed time is a
    // representation deviation the drilldown parser is sensitive to.
    let as_string =
        json!({ "dataIngested": true, "oldestProfileTime": "1000", "newestProfileTime": "2000" });
    let as_number =
        json!({ "dataIngested": true, "oldestProfileTime": 1000, "newestProfileTime": 2000 });

    let err = assert_get_profile_stats_compatible(&as_string, &as_number).unwrap_err();
    assert!(
        err.to_string().contains("JSON representation differs"),
        "{err}"
    );
}

#[test]
fn profile_stats_differential_accepts_divergent_timestamps() {
    // The two backends ingest at independent wall-clock instants, so differing
    // timestamp magnitudes (same string representation) must NOT be a difference.
    let pyroscope =
        json!({ "dataIngested": true, "oldestProfileTime": "111", "newestProfileTime": "222" });
    let krabka =
        json!({ "dataIngested": true, "oldestProfileTime": "999", "newestProfileTime": "1234" });

    assert_get_profile_stats_compatible(&pyroscope, &krabka).unwrap();
}

#[test]
fn profile_stats_differential_rejects_data_ingested_key_casing_drift() {
    let camel = json!({ "dataIngested": true });
    let snake = json!({ "data_ingested": true });

    let err = assert_get_profile_stats_compatible(&camel, &snake).unwrap_err();
    assert!(
        err.to_string().contains("dataIngested wire key differs"),
        "{err}"
    );
}

#[test]
fn connect_diff_differential_rejects_tick_drift() {
    let expected = json!({
        "flamegraph": {
            "names": ["total", "main"],
            "levels": [{ "values": [0, 7, 0, 0, 7, 0, 0] }],
            "total": 14,
            "maxSelf": 0,
            "leftTicks": 7,
            "rightTicks": 7
        }
    });
    let actual = json!({
        "flamegraph": {
            "names": ["total", "main"],
            "levels": [{ "values": [0, 7, 0, 0, 8, 0, 0] }],
            "total": 15,
            "maxSelf": 0,
            "leftTicks": 7,
            "rightTicks": 8
        }
    });

    let err = assert_diff_equal(&expected, &actual).unwrap_err();
    assert!(err.to_string().contains("Diff mismatch"));
    assert_diff_equal(&expected, &expected).unwrap();
    let mut swapped_names = expected.clone();
    swapped_names["flamegraph"]["names"] = json!(["main", "total"]);
    assert!(assert_diff_equal(&expected, &swapped_names).is_err());
    let mut swapped_index = expected.clone();
    swapped_index["flamegraph"]["levels"][0]["values"][6] = json!(1);
    assert!(assert_diff_equal(&expected, &swapped_index).is_err());
    let mut swapped_values = expected.clone();
    swapped_values["flamegraph"]["levels"][0]["values"] = json!([0, 0, 7, 0, 7, 0, 0]);
    assert!(assert_diff_equal(&expected, &swapped_values).is_err());

    let unequal = json!({"flamegraph": {"names": ["total", FUNC_WORK, FUNC_HOT],
        "levels": [{"values": [0, 140, 0, 0, 28, 0, 0]},
            {"values": [0, 140, 40, 0, 28, 8, 1]},
            {"values": [40, 100, 100, 8, 20, 20, 2]}],
        "total": 168, "maxSelf": 100, "leftTicks": 140, "rightTicks": 28}});
    assert_eq!(
        canonical_diff(&unequal).unwrap(),
        populated_diff_expected(false)
    );
    let mut swapped_sides = unequal.clone();
    swapped_sides["flamegraph"]["levels"] = json!([
        {"values": [0, 28, 0, 0, 140, 0, 0]},
        {"values": [0, 28, 8, 0, 140, 40, 1]},
        {"values": [8, 20, 20, 40, 100, 100, 2]}]);
    swapped_sides["flamegraph"]["leftTicks"] = json!(28);
    swapped_sides["flamegraph"]["rightTicks"] = json!(140);
    assert_eq!(
        canonical_diff(&swapped_sides).unwrap(),
        populated_diff_expected(true)
    );
    assert!(assert_diff_equal(&unequal, &swapped_sides).is_err());
    let mut stale_sides = unequal.clone();
    stale_sides["flamegraph"]["levels"] = swapped_sides["flamegraph"]["levels"].clone();
    assert!(assert_diff_equal(&unequal, &stale_sides).is_err());
}

#[test]
fn diff_comparison_resolves_layout_but_preserves_stack_value_associations() -> TestResult {
    let expected = json!({"flamegraph": {
        "names": ["total", "a", "b", "shared"],
        "levels": [
            {"values": [0, 10, 0, 0, 10, 0, 0]},
            {"values": [0, 6, 2, 0, 6, 2, 1, 0, 4, 1, 0, 4, 1, 2]},
            {"values": [2, 4, 4, 2, 4, 4, 3, 1, 3, 3, 1, 3, 3, 3]}],
        "total": 20, "maxSelf": 4, "leftTicks": 10, "rightTicks": 10}});
    let mut permuted = expected.clone();
    permuted["flamegraph"]["names"] = json!(["total", "b", "shared", "a"]);
    // Move b before a on both sides, update all name indices, and move each
    // descendant with its parent. The shared leaf's parent remains observable.
    permuted["flamegraph"]["levels"][1]["values"] =
        json!([0, 4, 1, 0, 4, 1, 1, 0, 6, 2, 0, 6, 2, 3]);
    permuted["flamegraph"]["levels"][2]["values"] =
        json!([1, 3, 3, 1, 3, 3, 2, 2, 4, 4, 2, 4, 4, 2]);
    assert_diff_equal(&expected, &permuted)?;

    let mut swapped_names = expected.clone();
    swapped_names["flamegraph"]["names"] = json!(["total", "b", "a", "shared"]);
    assert2::assert!(assert_diff_equal(&expected, &swapped_names).is_err());
    let mut corrupted_value = expected.clone();
    corrupted_value["flamegraph"]["levels"][2]["values"][2] = json!(3);
    assert2::assert!(assert_diff_equal(&expected, &corrupted_value).is_err());
    let mut malformed = expected.clone();
    malformed["flamegraph"]["levels"][1]["values"] = json!([0, 6, 2, 0, 6, 2]);
    assert2::assert!(canonical_diff(&malformed).is_err());
    let mut invalid_index = expected.clone();
    invalid_index["flamegraph"]["levels"][1]["values"][6] = json!(999);
    assert2::assert!(canonical_diff(&invalid_index).is_err());
    let mut outside_parent = expected.clone();
    outside_parent["flamegraph"]["levels"][2]["values"][0] = json!(11);
    assert2::assert!(canonical_diff(&outside_parent).is_err());
    let mut invalid_width = expected.clone();
    invalid_width["flamegraph"]["levels"][1]["values"][1] = json!(-1);
    assert2::assert!(canonical_diff(&invalid_width).is_err());

    // Preserve multiplicity even when two identical zero-width bars share a
    // path. Sorting whole records must not turn the result into a set.
    let duplicate = json!({"flamegraph": {"names": ["total", "zero"],
        "levels": [{"values": [0, 1, 1, 0, 1, 1, 0]},
            {"values": [1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]}],
        "total": 2, "maxSelf": 1, "leftTicks": 1, "rightTicks": 1}});
    let mut single = duplicate.clone();
    single["flamegraph"]["levels"][1]["values"] = json!([1, 0, 0, 1, 0, 0, 1]);
    assert2::assert!(assert_diff_equal(&duplicate, &single).is_err());
    Ok(())
}

// ---------------------------------------------------------------------------
// Comprehensive Grafana end-to-end test
//
// Unlike `grafana_accepts_pyroscope_datasource_pointing_at_krabka` (which only
// registers a datasource and reads it back), this test drives the *full* path:
// ingest a known profile through the real distributor push door, then stand up
// real Grafana with its built-in Pyroscope datasource pointed at Krabka and
// prove that Grafana → grafana-pyroscope-datasource → Krabka works for
//   (1) the config-test / health probe (ProfileTypes through the plugin),
//   (2) a flamegraph query driven *through* Grafana (the real Explore path),
//   (3) multi-tenant isolation enforced through Grafana's per-datasource
//       X-Scope-OrgID header injection.
// ---------------------------------------------------------------------------

/// Every public querier RPC accepts binary Connect messages and responds in
/// kind. This also guards the Grafana datasource's connect-go client path.
#[tokio::test]
async fn every_querier_rpc_accepts_binary_connect() -> TestResult {
    let store = WalTailProfileStore::new();
    let krabka = start_krabka_public(CapturingSink::default(), store).await?;
    let client = reqwest::Client::new();
    let stack_query = pb::querier::v1::SelectMergeStacktracesRequest {
        profile_type_id: PROFILE_TYPE.to_string(),
        label_selector: SELECTOR.to_string(),
        start: 0,
        end: 100,
        ..Default::default()
    };
    let cases = vec![
        (
            "ProfileTypes",
            prost::Message::encode_to_vec(&pb::querier::v1::ProfileTypesRequest {
                start: 0,
                end: 100,
            }),
        ),
        (
            "LabelNames",
            prost::Message::encode_to_vec(&pb::querier::v1::LabelNamesRequest {
                matchers: vec![SELECTOR.to_string()],
                start: 0,
                end: 100,
            }),
        ),
        (
            "LabelValues",
            prost::Message::encode_to_vec(&pb::querier::v1::LabelValuesRequest {
                name: "env".to_string(),
                matchers: vec![SELECTOR.to_string()],
                start: 0,
                end: 100,
            }),
        ),
        (
            "Series",
            prost::Message::encode_to_vec(&pb::querier::v1::SeriesRequest {
                matchers: vec![SELECTOR.to_string()],
                label_names: Vec::new(),
                start: 0,
                end: 100,
            }),
        ),
        (
            "SelectMergeStacktraces",
            prost::Message::encode_to_vec(&stack_query),
        ),
        (
            "SelectMergeSpanProfile",
            prost::Message::encode_to_vec(&pb::querier::v1::SelectMergeSpanProfileRequest {
                profile_type_id: PROFILE_TYPE.to_string(),
                label_selector: SELECTOR.to_string(),
                span_selector: vec!["000000000000002a".to_string()],
                start: 0,
                end: 100,
                ..Default::default()
            }),
        ),
        (
            "SelectMergeProfile",
            prost::Message::encode_to_vec(&pb::querier::v1::SelectMergeProfileRequest {
                profile_type_id: PROFILE_TYPE.to_string(),
                label_selector: SELECTOR.to_string(),
                start: 0,
                end: 100,
                ..Default::default()
            }),
        ),
        (
            "SelectSeries",
            prost::Message::encode_to_vec(&pb::querier::v1::SelectSeriesRequest {
                profile_type_id: PROFILE_TYPE.to_string(),
                label_selector: SELECTOR.to_string(),
                start: 0,
                end: 100,
                step: 1.0,
                ..Default::default()
            }),
        ),
        (
            "SelectHeatmap",
            prost::Message::encode_to_vec(&pb::querier::v1::SelectHeatmapRequest {
                profile_type_id: PROFILE_TYPE.to_string(),
                label_selector: SELECTOR.to_string(),
                start: 0,
                end: 100,
                step: 1.0,
                ..Default::default()
            }),
        ),
        (
            "Diff",
            prost::Message::encode_to_vec(&pb::querier::v1::DiffRequest {
                left: Some(stack_query.clone()),
                right: Some(stack_query),
            }),
        ),
        (
            "GetProfileStats",
            prost::Message::encode_to_vec(&pb::querier::v1::GetProfileStatsRequest {}),
        ),
        (
            "AnalyzeQuery",
            prost::Message::encode_to_vec(&pb::querier::v1::AnalyzeQueryRequest {
                start: 0,
                end: 100,
                query: format!("{PROFILE_TYPE}{SELECTOR}"),
            }),
        ),
    ];

    for (method, request_body) in cases {
        let response = client
            .post(format!(
                "http://127.0.0.1:{}/querier.v1.QuerierService/{method}",
                krabka.querier_port
            ))
            .header(reqwest::header::CONTENT_TYPE, "application/proto")
            .header("x-scope-orgid", TENANT)
            .body(request_body)
            .send()
            .await?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = response.bytes().await?;
        assert!(
            status.is_success(),
            "{method} (application/proto) returned {status}: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(
            content_type.starts_with("application/proto"),
            "{method} response must echo application/proto, got `{content_type}`"
        );
    }

    krabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/grafana image"]
async fn grafana_renders_krabka_profiles_end_to_end() -> TestResult {
    let client = reqwest::Client::new();

    let sample_time_ns = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())
        .map_err(|_| "current time does not fit i64 nanoseconds")?;
    let now_ms = sample_time_ns / 1_000_000;
    let from_ms = now_ms - 3_600_000;
    let to_ms = now_ms + 3_600_000;

    // 1. Ingest a known CPU profile for tenant-a through the real distributor push door,
    //    then replay the captured WAL records into the querier's hot store.
    let gzipped = synthetic_cpu_pprof(sample_time_ns)?;
    let sink = CapturingSink::default();
    let store = WalTailProfileStore::new();
    let krabka = start_krabka_public(sink.clone(), store.clone()).await?;
    post_cpu_profile(&client, &krabka.distributor_base, Some(TENANT), &gzipped).await?;
    drain_sink_into_cold_store(&sink)?;

    // 2. Real Grafana + its built-in Pyroscope datasource, one per tenant. Each datasource
    //    injects its own X-Scope-OrgID via the standard custom-HTTP-header mechanism, so the
    //    backend plugin tags every outgoing request to Krabka with the tenant.
    let grafana = start_grafana().await?;
    let grafana_base = mapped_base_url(&grafana, 3000).await?;
    wait_for_http_ok(&client, &grafana_base, &["/api/health"]).await?;
    let krabka_url = format!("http://host.docker.internal:{}", krabka.querier_port);
    let uid_a = create_pyroscope_datasource(
        &client,
        &grafana_base,
        "Krabka Profiles A",
        &krabka_url,
        TENANT,
    )
    .await?;
    let uid_b = create_pyroscope_datasource(
        &client,
        &grafana_base,
        "Krabka Profiles B",
        &krabka_url,
        TENANT_B,
    )
    .await?;

    // 3. Config-test / health probe: Grafana's datasource health check drives ProfileTypes
    //    through the plugin to Krabka (the spec's health surface; there is no /ready).
    let health = datasource_health_until_ok(&client, &grafana_base, &uid_a).await?;
    assert!(
        datasource_health_is_ok(&health),
        "tenant-a datasource health not OK: {health}"
    );

    // 4. Drive a flamegraph query THROUGH Grafana and assert Krabka's symbolized data returns.
    let query_a = GrafanaQuery {
        grafana_base: &grafana_base,
        uid: &uid_a,
        profile_type: CPU_PROFILE_TYPE,
        selector: E2E_SELECTOR,
        from_ms,
        to_ms,
    };
    let (names_a, positive_a) =
        grafana_profile_evidence_until(&client, &query_a, |names, positive| {
            positive && names.contains(FUNC_WORK)
        })
        .await?;
    for func in [FUNC_WORK, FUNC_HOT] {
        assert!(
            names_a.contains(func),
            "Grafana query must return {func}: {names_a:?}"
        );
    }
    assert!(
        positive_a,
        "Grafana query must return a positive sample value: {names_a:?}"
    );

    // 5. Multi-tenant isolation THROUGH Grafana: tenant-b's datasource must not see any of
    //    tenant-a's profiles, labels, or frames.
    let query_b = GrafanaQuery {
        grafana_base: &grafana_base,
        uid: &uid_b,
        profile_type: CPU_PROFILE_TYPE,
        selector: E2E_SELECTOR,
        from_ms,
        to_ms,
    };
    let (names_b, positive_b) = grafana_profile_evidence(&client, &query_b).await?;
    assert!(
        !names_b.contains(FUNC_WORK) && !names_b.contains(FUNC_HOT),
        "tenant-b leaked tenant-a frames through Grafana: {names_b:?}"
    );
    assert!(
        !positive_b,
        "tenant-b saw tenant-a sample values through Grafana"
    );

    krabka.shutdown();
    Ok(())
}

/// Builds a small, deterministic single-sample-type CPU pprof in gzip form. It
/// holds the two known functions `main.work` and `main.hotloop`, so the
/// flamegraph names are assertable.
fn synthetic_cpu_pprof(time_nanos: i64) -> TestResult<Vec<u8>> {
    synthetic_cpu_pprof_with_values(time_nanos, [100, 40])
}

fn synthetic_cpu_pprof_with_values(time_nanos: i64, values: [i64; 2]) -> TestResult<Vec<u8>> {
    let [hot_value, work_value] = values;
    gzip_bytes(
        &synthetic_cpu_profile::SyntheticCpuProfile {
            time_nanos,
            hot_value,
            work_value,
        }
        .encode(),
    )
}

fn gzip_bytes(bytes: &[u8]) -> TestResult<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

async fn post_cpu_profile(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    gzipped_pprof: &[u8],
) -> TestResult {
    post_cpu_profile_with_id(
        client,
        PushTarget { base, tenant },
        IdentifiedCpuProfile {
            gzipped_pprof,
            profile_id: "krabka-grafana-e2e",
        },
    )
    .await
}

/// A gzipped CPU pprof profile, pushed under the sample `ID` `profile_id`.
#[derive(Clone, Copy)]
struct IdentifiedCpuProfile<'a> {
    gzipped_pprof: &'a [u8],
    profile_id: &'a str,
}

async fn post_cpu_profile_with_id(
    client: &reqwest::Client,
    target: PushTarget<'_>,
    profile: IdentifiedCpuProfile<'_>,
) -> TestResult {
    let PushTarget { base, tenant } = target;
    let IdentifiedCpuProfile {
        gzipped_pprof,
        profile_id,
    } = profile;
    let body = json!({
        "series": [{
            "labels": [
                { "name": "__name__", "value": CPU_NAME },
                { "name": "service_name", "value": E2E_SERVICE },
                { "name": "env", "value": "e2e" }
            ],
            "samples": [{
                "rawProfile": BASE64.encode(gzipped_pprof),
                "ID": profile_id
            }]
        }]
    });
    post_push_json(
        client,
        PushJson {
            base,
            tenant,
            body: &body,
            what: "cpu profile",
        },
    )
    .await
}

struct KrabkaPublic {
    distributor_base: String,
    querier_port: u16,
    distributor_shutdown: Option<oneshot::Sender<()>>,
    querier_shutdown: Option<oneshot::Sender<()>>,
}

impl KrabkaPublic {
    fn shutdown(mut self) {
        if let Some(tx) = self.distributor_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.querier_shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// Like `start_krabka_pair`, but binds the querier on all interfaces so the
/// Grafana container can reach it at `host.docker.internal:<port>`. The
/// distributor stays host-local, because the test pushes to it directly.
async fn start_krabka_public(
    sink: CapturingSink,
    store: WalTailProfileStore,
) -> TestResult<KrabkaPublic> {
    let cold = sink.cold.clone();
    let (distributor_addr, distributor_shutdown) = start_distributor(sink).await?;

    let (querier_shutdown, querier_rx) = oneshot::channel();
    let querier_state = Arc::new(unbounded_querier_state(ProfileTiers { hot: store, cold }));
    let querier_addr = query::serve(
        "0.0.0.0:0".parse()?,
        querier_state,
        &ServerSecurity::default(),
        async move {
            let _ = querier_rx.await;
        },
    )
    .await?;

    Ok(KrabkaPublic {
        distributor_base: format!("http://{distributor_addr}"),
        querier_port: querier_addr.port(),
        distributor_shutdown: Some(distributor_shutdown),
        querier_shutdown: Some(querier_shutdown),
    })
}

async fn create_pyroscope_datasource(
    client: &reqwest::Client,
    grafana_base: &str,
    name: &str,
    krabka_url: &str,
    tenant: &str,
) -> TestResult<String> {
    let payload = json!({
        "name": name,
        "type": "grafana-pyroscope-datasource",
        "access": "proxy",
        "url": krabka_url,
        "jsonData": { "httpHeaderName1": "X-Scope-OrgID" },
        "secureJsonData": { "httpHeaderValue1": tenant }
    });
    let created: Value = client
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth("admin", Some("admin"))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    created
        .get("datasource")
        .and_then(|datasource| datasource.get("uid"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("datasource create response missing uid: {created}").into())
}

async fn datasource_health_until_ok(
    client: &reqwest::Client,
    grafana_base: &str,
    uid: &str,
) -> TestResult<Value> {
    let mut last = None;
    for _ in 0..120 {
        match datasource_health(client, grafana_base, uid).await {
            Ok(value) if datasource_health_is_ok(&value) => return Ok(value),
            Ok(value) => last = Some(format!("datasource health not OK: {value}")),
            Err(err) => last = Some(err.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Err(last
        .unwrap_or_else(|| "datasource health never became OK".to_string())
        .into())
}

async fn datasource_health(
    client: &reqwest::Client,
    grafana_base: &str,
    uid: &str,
) -> TestResult<Value> {
    let response = client
        .get(format!("{grafana_base}/api/datasources/uid/{uid}/health"))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(format!("datasource health returned {status}: {text}").into());
    }
    serde_json::from_str(&text)
        .map_err(|err| format!("datasource health returned non-JSON `{text}`: {err}").into())
}

fn datasource_health_is_ok(value: &Value) -> bool {
    value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("ok"))
}

struct GrafanaQuery<'a> {
    grafana_base: &'a str,
    uid: &'a str,
    profile_type: &'a str,
    selector: &'a str,
    from_ms: i64,
    to_ms: i64,
}

/// Collects the function names and a positive-value flag that a profile query
/// returns through Grafana.
///
/// The function tries the real `/api/ds/query` Explore path first, where the
/// backend plugin applies the X-Scope-OrgID header of the datasource. It then
/// tries the data-source proxy to the Krabka flamebearer as a best-effort
/// second source. It returns the union of both.
async fn grafana_profile_evidence(
    client: &reqwest::Client,
    query: &GrafanaQuery<'_>,
) -> TestResult<(BTreeSet<String>, bool)> {
    let mut names = BTreeSet::new();
    let mut positive = false;

    if let Ok(value) = ds_query_profile(client, query).await {
        let (frame_names, frame_positive) = evidence_from_ds_query(&value);
        names.extend(frame_names);
        positive = positive || frame_positive;
    }

    if let Some(value) = proxy_render(client, query).await {
        names.extend(flame_names(&value));
        positive = positive || flame_ticks(&value).is_some_and(|ticks| ticks > 0);
    }

    Ok((names, positive))
}

async fn grafana_profile_evidence_until(
    client: &reqwest::Client,
    query: &GrafanaQuery<'_>,
    ready: impl Fn(&BTreeSet<String>, bool) -> bool,
) -> TestResult<(BTreeSet<String>, bool)> {
    let mut last = None;
    for _ in 0..120 {
        match grafana_profile_evidence(client, query).await {
            Ok((names, positive)) if ready(&names, positive) => return Ok((names, positive)),
            Ok((names, positive)) => {
                last = Some(format!(
                    "evidence not ready: names={names:?} positive={positive}"
                ));
            }
            Err(err) => last = Some(err.to_string()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Err(last
        .unwrap_or_else(|| "Grafana profile evidence never became ready".to_string())
        .into())
}

async fn ds_query_profile(client: &reqwest::Client, query: &GrafanaQuery<'_>) -> TestResult<Value> {
    let body = json!({
        "from": query.from_ms.to_string(),
        "to": query.to_ms.to_string(),
        "queries": [{
            "refId": "A",
            "datasource": { "type": "grafana-pyroscope-datasource", "uid": query.uid },
            "queryType": "profile",
            "profileTypeId": query.profile_type,
            "labelSelector": query.selector,
            "groupBy": [],
            "maxNodes": 8192,
            "intervalMs": 60000,
            "maxDataPoints": 1000
        }]
    });
    let response = client
        .post(format!("{}/api/ds/query", query.grafana_base))
        .basic_auth("admin", Some("admin"))
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Err(format!("/api/ds/query returned {status}: {text}").into());
    }
    serde_json::from_str(&text)
        .map_err(|err| format!("/api/ds/query returned non-JSON `{text}`: {err}").into())
}

/// Walks the Grafana dataframe response column-major. It collects every string
/// cell as a candidate frame name and flags any strictly-positive numeric cell.
/// The walk is schema-agnostic, so it tolerates Grafana version drift in field
/// names.
fn evidence_from_ds_query(value: &Value) -> (BTreeSet<String>, bool) {
    let mut names = BTreeSet::new();
    let mut positive = false;
    let Some(results) = value.get("results").and_then(Value::as_object) else {
        return (names, positive);
    };
    for result in results.values() {
        let Some(frames) = result.get("frames").and_then(Value::as_array) else {
            continue;
        };
        for frame in frames {
            let Some(columns) = frame.pointer("/data/values").and_then(Value::as_array) else {
                continue;
            };
            for column in columns {
                let Some(cells) = column.as_array() else {
                    continue;
                };
                for cell in cells {
                    if let Some(text) = cell.as_str() {
                        names.insert(text.to_string());
                    } else if cell.as_f64().is_some_and(|number| number > 0.0) {
                        positive = true;
                    }
                }
            }
        }
    }
    (names, positive)
}

/// Queries Krabka's legacy flamebearer render through Grafana's data-source
/// proxy on a best-effort basis. Returns `None` if the proxy route is not
/// available. The `/api/ds/query` path then carries the test.
async fn proxy_render(client: &reqwest::Client, query: &GrafanaQuery<'_>) -> Option<Value> {
    let render_query = format!("{}{}", query.profile_type, query.selector);
    let encoded = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("query", &render_query)
        .append_pair("from", &query.from_ms.to_string())
        .append_pair("until", &query.to_ms.to_string())
        .append_pair("format", "json")
        .finish();
    let response = client
        .get(format!(
            "{}/api/datasources/proxy/uid/{}/pyroscope/render?{encoded}",
            query.grafana_base, query.uid
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    response.json().await.ok()
}

// -- Legacy `/ingest` differential ----------------------------------------

/// One upload through the legacy `/ingest` door, sent byte for byte to both
/// backends.
struct LegacyIngestCase {
    /// The `?name=` application, unique per case so the series never collide.
    app: &'static str,
    format: &'static str,
    content_type: &'static str,
    /// The profile type the upload is expected to land as, in both backends.
    profile_type: &'static str,
    body: Vec<u8>,
}

/// The stack formats all describe the same two-frame profile: 100 counts of
/// `main.hotloop` under `main.work`, and 40 of `main.work` on its own. Sending
/// one shape through four encodings makes a decoder that drops a frame or
/// mis-sums a value visible as a difference from Pyroscope rather than as a
/// difference between the cases.
fn legacy_ingest_cases(goroutine_pprof: &[u8]) -> Vec<LegacyIngestCase> {
    vec![
        LegacyIngestCase {
            app: "krabkadifffolded",
            format: "groups",
            content_type: "text/plain",
            profile_type: CPU_PROFILE_TYPE,
            body: b"main.work;main.hotloop 100\nmain.work 40\n".to_vec(),
        },
        LegacyIngestCase {
            app: "krabkadifflines",
            format: "lines",
            content_type: "text/plain",
            profile_type: CPU_PROFILE_TYPE,
            // One line per sample, so the same shape needs 140 of them.
            body: lines_body(),
        },
        LegacyIngestCase {
            app: "krabkadifftrie",
            format: "trie",
            content_type: "application/octet-stream",
            profile_type: CPU_PROFILE_TYPE,
            // A trie node's key is its parent's key plus this suffix, so the
            // leaf carries only `;main.hotloop`.
            body: stack_node(
                b"",
                0,
                &[stack_node(
                    b"main.work",
                    40,
                    &[stack_node(b";main.hotloop", 100, &[])],
                )],
            ),
        },
        LegacyIngestCase {
            app: "krabkadifftree",
            format: "tree",
            content_type: "application/octet-stream",
            profile_type: CPU_PROFILE_TYPE,
            // A tree node carries its own name, not a key suffix.
            body: stack_node(
                b"",
                0,
                &[stack_node(
                    b"main.work",
                    40,
                    &[stack_node(b"main.hotloop", 100, &[])],
                )],
            ),
        },
        LegacyIngestCase {
            app: "krabkadiffjfrcpu",
            format: "jfr",
            content_type: "multipart/form-data; boundary=krabka-jfr-boundary",
            profile_type: CPU_PROFILE_TYPE,
            body: jfr_body(),
        },
        LegacyIngestCase {
            app: "krabkadiffjfrwall",
            format: "jfr",
            content_type: "multipart/form-data; boundary=krabka-jfr-boundary",
            profile_type: WALL_PROFILE_TYPE,
            body: jfr_body(),
        },
        LegacyIngestCase {
            app: "krabkadiffspeedscope",
            format: "speedscope",
            content_type: "application/json",
            profile_type: CPU_PROFILE_TYPE,
            body: br#"{
              "$schema": "https://www.speedscope.app/file-format-schema.json",
              "shared": {"frames": [{"name": "main.work"}, {"name": "main.hotloop"}]},
              "profiles": [{
                "type": "sampled",
                "name": "cpu",
                "unit": "nanoseconds",
                "startValue": 0,
                "endValue": 140,
                "samples": [[0, 1], [0]],
                "weights": [100, 40]
              }]
            }"#
            .to_vec(),
        },
        LegacyIngestCase {
            app: "krabkadiffpprof",
            format: "pprof",
            // A raw pprof body, which is what the SDKs that do not speak
            // `push.v1` post. Pyroscope names the series after the profile's
            // own sample type rather than after `?name=`.
            content_type: "binary/octet-stream",
            profile_type: PROFILE_TYPE,
            body: goroutine_pprof.to_vec(),
        },
    ]
}

fn jfr_body() -> Vec<u8> {
    const BOUNDARY: &str = "krabka-jfr-boundary";
    let mut body = format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"jfr\"; filename=\"profile.jfr\"\r\nContent-Type: application/octet-stream\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(include_bytes!("fixtures/profiler-wall.jfr"));
    body.extend_from_slice(format!("\r\n--{BOUNDARY}--\r\n").as_bytes());
    body
}

/// 100 `main.work;main.hotloop` lines and 40 `main.work` lines, one sample per
/// line, which is what the `lines` format means.
fn lines_body() -> Vec<u8> {
    let mut body = String::new();
    for _ in 0..100 {
        body.push_str("main.work;main.hotloop\n");
    }
    for _ in 0..40 {
        body.push_str("main.work\n");
    }
    body.into_bytes()
}

/// One node of Pyroscope's `trie` and `tree` payloads, which share a shape:
/// a length-prefixed label, the node's own value, and its children.
fn stack_node(label: &[u8], value: u64, children: &[Vec<u8>]) -> Vec<u8> {
    let mut out = uvarint(label.len() as u64);
    out.extend_from_slice(label);
    out.extend(uvarint(value));
    out.extend(uvarint(children.len() as u64));
    for child in children {
        out.extend_from_slice(child);
    }
    out
}

fn uvarint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = u8::try_from(value & 0x7f).expect("seven bits fit a byte");
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

async fn post_ingest(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    case: &LegacyIngestCase,
) -> TestResult {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .to_string();
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query
        .append_pair("name", case.app)
        .append_pair("format", case.format)
        .append_pair("sampleRate", "100")
        .append_pair("from", &now_secs)
        .append_pair("until", &now_secs);
    if case.format == "tree" {
        // Pyroscope's v2 adapter accepts but ignores this legacy storage hint.
        query.append_pair("aggregationType", "average");
    }
    let query = query.finish();
    let request = client
        .post(format!("{base}/ingest?{query}"))
        .header(reqwest::header::CONTENT_TYPE, case.content_type)
        .body(case.body.clone());
    send_expecting_ok(
        request,
        &ExpectedOk {
            tenant,
            what: &format!("/ingest format={} to {base}", case.format),
        },
    )
    .await
}

async fn assert_legacy_failure_statuses_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    krabka_base: &str,
) -> TestResult {
    for (name, query, content_type, body) in [
        (
            "missing name",
            "format=groups",
            "text/plain",
            &b"main 1\n"[..],
        ),
        (
            "malformed groups",
            "name=badgroups&format=groups",
            "text/plain",
            &b"main nope\n"[..],
        ),
        (
            "malformed pprof",
            "name=badpprof&format=pprof",
            "application/octet-stream",
            &b"not a pprof"[..],
        ),
        (
            "malformed speedscope",
            "name=badspeedscope&format=speedscope",
            "application/json",
            &b"{}"[..],
        ),
        (
            "missing jfr part",
            "name=badjfr&format=jfr",
            "multipart/form-data; boundary=empty",
            &b"--empty--\r\n"[..],
        ),
    ] {
        let send = |base: &str, tenant: Option<&str>| {
            let mut request = client
                .post(format!("{base}/ingest?{query}"))
                .header(reqwest::header::CONTENT_TYPE, content_type)
                .body(body.to_vec());
            if let Some(tenant) = tenant {
                request = request.header("x-scope-orgid", tenant);
            }
            request.send()
        };
        let pyroscope = send(pyroscope_base, None).await?;
        let krabka = send(krabka_base, Some(TENANT)).await?;
        if pyroscope.status() != krabka.status() {
            return Err(format!(
                "legacy {name} status differs: pyroscope={} body={:?}, krabka={} body={:?}",
                pyroscope.status(),
                pyroscope.text().await.unwrap_or_default(),
                krabka.status(),
                krabka.text().await.unwrap_or_default(),
            )
            .into());
        }
    }
    Ok(())
}

/// Replay each captured record once into the cold head.
///
/// One head preserves the original fixture's global time bounds for v1 query
/// analysis. The capture has no Kafka positions, so these rows have no WAL
/// identities. Positioned overlap and mixed hot/cold downsample contributors
/// are checked by the real WAL and compaction tests; this corpus checks the
/// canonical cold-only union against the native API.
fn drain_sink_into_cold_store(sink: &CapturingSink) -> TestResult {
    let records = sink
        .records
        .lock()
        .map_err(|_| "capturing sink lock poisoned")?
        .clone();
    let count = records.len();
    for record in records {
        sink.cold.append_record(record)?;
    }
    eprintln!("native union replay: hot=0 cold={count}");
    Ok(())
}

/// Every legacy `/ingest` format, ingested byte for byte into real Pyroscope
/// and into krabka, then rendered from both and compared.
///
/// `tests/pyroscope_differential.rs` covered only `push.v1` before this, which
/// left the whole of `src/ingest/legacy` measured against krabka's own idea of
/// each wire format. It differed from Pyroscope's: a stack upload is a CPU
/// profile in nanoseconds named `process_cpu`, whatever `?units=` says, and
/// its counts are stored as the time they stand for.
///
/// Pyroscope 2.3.1's default image accepts a sampled speedscope upload but
/// exposes no profile for it. The explicit branch below records that known
/// output divergence while keeping Krabka's useful speedscope ingestion.
#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/pyroscope image"]
async fn real_pyroscope_legacy_ingest_formats_match_krabka() -> TestResult {
    let client = reqwest::Client::new();
    let pyroscope = start_pyroscope().await?;
    let pyroscope_base = ready_pyroscope_base(&client, &pyroscope).await?;
    let goroutine_pprof = fetch_goroutine_pprof(&client, &pyroscope_base).await?;

    let sink = CapturingSink::default();
    let store = WalTailProfileStore::new();
    let krabka = start_krabka_pair(sink.clone(), store.clone()).await?;

    let cases = legacy_ingest_cases(&goroutine_pprof);
    for case in &cases {
        post_ingest(&client, &pyroscope_base, None, case).await?;
        post_ingest(&client, &krabka.distributor_base, Some(TENANT), case).await?;
    }
    drain_sink_into_cold_store(&sink)?;

    for case in &cases {
        let selector = if case.format == "jfr" {
            format!(r#"{{service_name="{}",jfr_event="wall"}}"#, case.app)
        } else {
            format!(r#"{{service_name="{}"}}"#, case.app)
        };
        let query = format!("{}{selector}", case.profile_type);
        if case.format == "speedscope" {
            let pyroscope_render = render_any(
                &client,
                &pyroscope_base,
                std::slice::from_ref(&query),
                "now-1h",
                "now",
                None,
                false,
            )
            .await?;
            if flame_names(&pyroscope_render).len() > 1 {
                return Err(format!(
                    "Pyroscope 2.3.1 unexpectedly exposed the known-empty speedscope upload: {pyroscope_render}"
                )
                .into());
            }
            render_any(
                &client,
                &krabka.querier_base,
                std::slice::from_ref(&query),
                "0",
                &i64::MAX.to_string(),
                Some(TENANT),
                true,
            )
            .await?;
            continue;
        }
        let pyroscope_render = render_until_non_empty(
            &client,
            &pyroscope_base,
            std::slice::from_ref(&query),
            "now-1h",
            "now",
            None,
        )
        .await
        .map_err(|err| format!("pyroscope render for format={}: {err}", case.format))?;
        let krabka_render = render_any(
            &client,
            &krabka.querier_base,
            &[query],
            "0",
            &i64::MAX.to_string(),
            Some(TENANT),
            false,
        )
        .await
        .map_err(|err| format!("krabka render for format={}: {err}", case.format))?;
        assert_flamebearer_equal(&pyroscope_render, &krabka_render)
            .map_err(|err| format!("/ingest format={}: {err}", case.format))?;
    }

    assert_legacy_failure_statuses_match(&client, &pyroscope_base, &krabka.distributor_base)
        .await?;

    krabka.shutdown();
    Ok(())
}

// -- Profile-type differential -------------------------------------------

/// One profile pushed through `push.v1`, plus the type it is queried back as.
struct ProfileTypeCase {
    app: &'static str,
    name: &'static str,
    profile_type: &'static str,
    gzipped_pprof: Vec<u8>,
}

/// The `push.v1` differential covered goroutine profiles alone, so every
/// sample type but `goroutine:count` went unmeasured. That includes the two
/// whose values are nanoseconds, and the one that carries four sample types in
/// a single profile. Each case here pushes the same bytes to both backends and
/// compares the flamegraph they answer with.
///
/// The allocation, mutex and block profiles are the Pyroscope container's own,
/// so they are real Go profiles rather than hand-built ones; the CPU profile is
/// synthetic because the container's `/debug/pprof/profile` is not enabled.
#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/pyroscope image"]
async fn real_pyroscope_profile_types_match_krabka() -> TestResult {
    let client = reqwest::Client::new();
    let pyroscope = start_pyroscope().await?;
    let pyroscope_base = ready_pyroscope_base(&client, &pyroscope).await?;

    let now_nanos = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let cases = vec![
        ProfileTypeCase {
            app: "krabkadiffcpu",
            name: CPU_NAME,
            profile_type: CPU_PROFILE_TYPE,
            gzipped_pprof: synthetic_cpu_pprof(now_nanos)?,
        },
        ProfileTypeCase {
            app: "krabkadiffalloc",
            name: "memory",
            profile_type: "memory:alloc_space:bytes:space:bytes",
            gzipped_pprof: fetch_debug_pprof(&client, &pyroscope_base, "allocs").await?,
        },
        ProfileTypeCase {
            app: "krabkadiffmutex",
            name: "mutex",
            profile_type: "mutex:delay:nanoseconds:contentions:count",
            gzipped_pprof: fetch_debug_pprof(&client, &pyroscope_base, "mutex").await?,
        },
        ProfileTypeCase {
            app: "krabkadiffblock",
            name: "block",
            profile_type: "block:delay:nanoseconds:contentions:count",
            gzipped_pprof: fetch_debug_pprof(&client, &pyroscope_base, "block").await?,
        },
        ProfileTypeCase {
            app: "krabkadiffgoroutines",
            name: "goroutines",
            profile_type: PROFILE_TYPE,
            gzipped_pprof: fetch_goroutine_pprof(&client, &pyroscope_base).await?,
        },
    ];

    let sink = CapturingSink::default();
    let store = WalTailProfileStore::new();
    let krabka = start_krabka_pair(sink.clone(), store.clone()).await?;

    for case in &cases {
        post_push_typed(&client, PushTarget::oracle(&pyroscope_base), case).await?;
        post_push_typed(&client, PushTarget::krabka(&krabka.distributor_base), case).await?;
    }
    drain_sink_into_cold_store(&sink)?;

    for case in &cases {
        let query = format!(r#"{}{{service_name="{}"}}"#, case.profile_type, case.app);
        let pyroscope_render = render_until_non_empty(
            &client,
            &pyroscope_base,
            std::slice::from_ref(&query),
            "now-1h",
            "now",
            None,
        )
        .await
        .map_err(|err| format!("pyroscope render for {}: {err}", case.profile_type))?;
        let krabka_render = render_any(
            &client,
            &krabka.querier_base,
            &[query],
            "0",
            &i64::MAX.to_string(),
            Some(TENANT),
            false,
        )
        .await
        .map_err(|err| format!("krabka render for {}: {err}", case.profile_type))?;
        assert_flamebearer_equal(&pyroscope_render, &krabka_render)
            .map_err(|err| format!("profile type {}: {err}", case.profile_type))?;
    }

    krabka.shutdown();
    Ok(())
}

async fn fetch_debug_pprof(
    client: &reqwest::Client,
    base: &str,
    kind: &str,
) -> TestResult<Vec<u8>> {
    Ok(client
        .get(format!("{base}/debug/pprof/{kind}?debug=0"))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec())
}

async fn post_push_typed(
    client: &reqwest::Client,
    target: PushTarget<'_>,
    case: &ProfileTypeCase,
) -> TestResult {
    let PushTarget { base, tenant } = target;
    let body = json!({
        "series": [{
            "labels": [
                { "name": "__name__", "value": case.name },
                { "name": "service_name", "value": case.app }
            ],
            "samples": [{
                "rawProfile": BASE64.encode(&case.gzipped_pprof),
                "ID": format!("krabka-differential-{}", case.app)
            }]
        }]
    });
    post_push_json(
        client,
        PushJson {
            base,
            tenant,
            body: &body,
            what: case.name,
        },
    )
    .await
}

// -- OTLP `v1development` differential ------------------------------------

/// The OTLP profiles door, exported byte for byte into real Pyroscope and into
/// krabka, then rendered from both and compared.
///
/// `src/ingest/otlp.rs` was measured only against fixtures built in its own
/// tests, so nothing checked that krabka reads the `v1development` tables the
/// way the component it replaces does. The export here is a protobuf built
/// from krabka's vendored copy of the schema and posted unchanged to both, so
/// a divergence in the dictionary indices, the stack ordering or the derived
/// series name shows up as a difference in the flamegraph.
#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/pyroscope image"]
async fn real_pyroscope_otlp_export_matches_krabka() -> TestResult {
    let client = reqwest::Client::new();
    let pyroscope = start_pyroscope().await?;
    let pyroscope_base = ready_pyroscope_base(&client, &pyroscope).await?;

    let sink = CapturingSink::default();
    let store = WalTailProfileStore::new();
    let krabka = start_krabka_pair(sink.clone(), store.clone()).await?;

    let now_nanos = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    let protobuf = otlp_export_body(now_nanos, OTLP_PROTOBUF_SERVICE, false)?;
    let json = otlp_export_body(now_nanos, OTLP_JSON_SERVICE, true)?;
    let gzip = gzip_bytes(&otlp_export_body(now_nanos, OTLP_GZIP_SERVICE, false)?)?;
    let cases = [
        (
            OTLP_PROTOBUF_SERVICE,
            "application/x-protobuf",
            None,
            protobuf,
        ),
        (OTLP_JSON_SERVICE, "application/json", None, json),
        (
            OTLP_GZIP_SERVICE,
            "application/protobuf",
            Some("gzip"),
            gzip,
        ),
    ];
    for (_, content_type, content_encoding, export) in &cases {
        post_otlp_export(
            &client,
            &pyroscope_base,
            None,
            export,
            content_type,
            *content_encoding,
        )
        .await?;
        post_otlp_export(
            &client,
            &krabka.distributor_base,
            Some(TENANT),
            export,
            content_type,
            *content_encoding,
        )
        .await?;
    }
    drain_sink_into_cold_store(&sink)?;

    for (service, _, _, _) in &cases {
        // Pyroscope names an OTLP series after the profile's own sample type,
        // so this is `cpu:...` where `/ingest` gives `process_cpu`.
        let query = format!(r#"{OTLP_PROFILE_TYPE}{{service_name="{service}"}}"#);
        let pyroscope_render = render_until_non_empty(
            &client,
            &pyroscope_base,
            std::slice::from_ref(&query),
            "now-1h",
            "now",
            None,
        )
        .await?;
        let krabka_render = render_any(
            &client,
            &krabka.querier_base,
            &[query],
            "0",
            &i64::MAX.to_string(),
            Some(TENANT),
            false,
        )
        .await?;
        assert_flamebearer_equal(&pyroscope_render, &krabka_render)?;
        assert_otlp_series_labels_match(&client, &pyroscope_base, &krabka.querier_base, service)
            .await?;
    }

    krabka.shutdown();
    Ok(())
}

/// The series an OTLP export produces, compared label for label. The
/// flamegraph alone would not notice a series named or tagged differently,
/// and Pyroscope tags an OTLP series `__otel__` on top of the meta labels
/// every ingest path writes.
async fn assert_otlp_series_labels_match(
    client: &reqwest::Client,
    pyroscope_base: &str,
    krabka_base: &str,
    service: &str,
) -> TestResult {
    let body = json!({
        "matchers": [format!(r#"{{service_name="{service}"}}"#)],
        "start": query_start_ms(),
        "end": query_end_ms(),
    });
    let pyroscope = connect_json_until(
        client,
        pyroscope_base,
        None,
        "Series",
        body.clone(),
        series_has_labelsets,
    )
    .await?;
    let krabka = connect_json(client, krabka_base, Some(TENANT), "Series", body).await?;

    let expected = series_label_sets(&pyroscope)
        .ok_or_else(|| format!("pyroscope Series response has no label sets: {pyroscope}"))?;
    let actual = series_label_sets(&krabka)
        .ok_or_else(|| format!("krabka Series response has no label sets: {krabka}"))?;
    if expected != actual {
        return Err(format!(
            "OTLP series label sets differ:\n  pyroscope={expected:?}\n  krabka={actual:?}"
        )
        .into());
    }
    Ok(())
}

const OTLP_PROTOBUF_SERVICE: &str = "krabkadiffotlp-protobuf";

/// Exercise the two remaining v1 RPCs with linked, populated profiles, rather
/// than accepting an empty-store response as semantic coverage. The pinned
/// v1 querier explicitly does not implement `SelectHeatmap`; that capability
/// boundary is checked below and requires a separate v2 oracle deployment.
#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/pyroscope image"]
async fn real_pyroscope_span_profiles_and_query_analysis_match_krabka() -> TestResult {
    let mut evidence = json!({
        "suite": "pyroscope-populated-rpcs",
        "status": "running",
        "planned": 113,
        "upstream": {
            "source_revision": "7aeaa0ff91e83538b3ff0d09bfefb168bddc022d",
            "image_tag": std::env::var("KRABKA_PYROSCOPE_IMAGE_TAG").ok(),
            "image_id": std::env::var("KRABKA_PYROSCOPE_IMAGE_ID").ok(),
            "query_analysis_series_enabled": true,
            "candidate_query_analysis_series_enabled": true,
            "self_profiling_disable_push": true,
            "storage": "v1",
        },
        "fixture": { "profiles": 4, "samples": 8, "linked_samples": 2,
            "profile_type": OTLP_PROFILE_TYPE },
        "oracle_capabilities": {"SelectHeatmap": {"status": "not-exercised", "expected": "unimplemented",
            "semantic_comparison": false,
            "source": "https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/pkg/querier/querier.go"}},
        "cases": [],
        "coverage_gaps": ["v2 SelectHeatmap values, groups and exemplars",
            "all-RPC selector and time field matrix", "stack selectors",
            "profile UUID selectors", "trace selectors", "format and maxNodes matrix",
            "groupBy, aggregation, limit and exemplar matrix"],
        "active_case": "oracle startup",
    });
    write_profile_rpc_evidence(&evidence)?;
    let mut result = populated_profile_rpc_comparisons(&mut evidence).await;
    let recorded = evidence["cases"]
        .as_array()
        .ok_or("profile evidence cases missing")?
        .len();
    if result.is_ok() && recorded != 113 {
        result = Err(format!(
            "populated RPC evidence incomplete: expected 113 cases, recorded {recorded}"
        )
        .into());
    }
    evidence["status"] = json!(if result.is_ok() { "passed" } else { "failed" });
    if let Err(error) = &result {
        evidence["error"] = json!(error.to_string());
    }
    write_profile_rpc_evidence(&evidence)?;
    result
}

fn write_profile_rpc_evidence(evidence: &Value) -> TestResult {
    if let Some(directory) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory)?;
        std::fs::write(
            directory.join("pyroscope-populated-rpcs.json"),
            serde_json::to_vec_pretty(evidence)?,
        )?;
    }
    Ok(())
}

fn record_profile_rpc_case(evidence: &mut Value, mut case: Value) -> TestResult {
    if case.get("request").is_none()
        && let Some(request) = evidence["active_case"].get("request")
    {
        case["request"] = request.clone();
    }
    let backend = case["backend"].as_str().unwrap_or("upstream");
    let role = evidence["backend_roles"]
        .get(backend)
        .and_then(Value::as_str)
        .unwrap_or(backend);
    let name = case["name"].as_str().unwrap_or_else(|| {
        if case["classification"] == "unsupported-oracle" {
            "v1 unavailable"
        } else if case["classification"] == "expected-divergence" {
            "excluded time"
        } else {
            "unnamed"
        }
    });
    case["id"] = json!(format!(
        "{}/{}/{}/{}",
        case["method"].as_str().unwrap_or("unknown"),
        name,
        case["transport"].as_str().unwrap_or("json"),
        role
    ));
    evidence["cases"]
        .as_array_mut()
        .ok_or("profile evidence cases missing")?
        .push(case);
    write_profile_rpc_evidence(evidence)
}

async fn populated_profile_rpc_comparisons(evidence: &mut Value) -> TestResult {
    use pb::otlp_profiles::{ExportProfilesServiceRequest, Link};
    use prost::Message;

    let client = reqwest::Client::new();
    // The image defaults to v1-v2-dual. Pin the storage architecture as well
    // as the image when exercising v1-only capability and analysis behavior.
    let pyroscope = start_pyroscope_with_options(&[
        "-architecture.storage=v1",
        "-write-path=ingester",
        "-querier.query-analysis-series-enabled=true",
        "-self-profiling.disable-push=true",
    ])
    .await?;
    let pyroscope_base = ready_pyroscope_base(&client, &pyroscope).await?;
    let sink = CapturingSink::default();
    let store = WalTailProfileStore::new();
    let krabka = start_krabka_pair(sink.clone(), store.clone()).await?;
    evidence["backend_roles"] = json!({(pyroscope_base.as_str()): "upstream", (krabka.querier_base.as_str()): "krabka-direct"});
    let ingestion_nanos = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;
    // Keep both timestamps inside the same minute (and hence the same v1
    // head), with an intervening empty window inside the head's time bounds.
    let profile_nanos = ingestion_nanos / 60_000_000_000 * 60_000_000_000 - 20_000_000_000;
    let fixture_timestamp = i64::try_from(profile_nanos / 1_000_000)?;
    let selector = r#"{service_name="krabkadiff-linked"}"#;
    let mut export = ExportProfilesServiceRequest::decode(
        otlp_export_body(profile_nanos, "krabkadiff-linked", false)?.as_slice(),
    )?;
    export
        .dictionary
        .as_mut()
        .ok_or("missing fixture dictionary")?
        .link_table = vec![
        Link::default(), // OTLP reserves index zero for the absence of a link.
        Link {
            trace_id: vec![1; 16],
            span_id: 42_u64.to_be_bytes().to_vec(),
        },
        Link {
            trace_id: vec![2; 16],
            span_id: 43_u64.to_be_bytes().to_vec(),
        },
    ];
    let samples = &mut export.resource_profiles[0].scope_profiles[0].profiles[0].samples;
    samples[0].link_index = 1;
    samples[1].link_index = 2;
    let bytes = export.encode_to_vec();
    let time_control = otlp_export_body(
        profile_nanos - 5_000_000_000,
        "krabkadiff-time-control",
        false,
    )?;
    let cpu_profile = synthetic_cpu_pprof(i64::try_from(profile_nanos)?)?;
    // Separate Diff sides by two seconds; existing populated RPC windows end
    // at fixture_timestamp + 1s and retain their original expected values.
    let cpu_right_profile =
        synthetic_cpu_pprof_with_values(i64::try_from(profile_nanos + 2_000_000_000)?, [20, 8])?;
    for (base, tenant) in [
        (&pyroscope_base, None),
        (&krabka.distributor_base, Some(TENANT)),
    ] {
        post_otlp_export(
            &client,
            base,
            tenant,
            &bytes,
            "application/x-protobuf",
            None,
        )
        .await?;
        post_cpu_profile(&client, base, tenant, &cpu_profile).await?;
        post_cpu_profile_with_id(
            &client,
            PushTarget { base, tenant },
            IdentifiedCpuProfile {
                gzipped_pprof: &cpu_right_profile,
                profile_id: "krabka-diff-right",
            },
        )
        .await?;
        post_otlp_export(
            &client,
            base,
            tenant,
            &time_control,
            "application/x-protobuf",
            None,
        )
        .await?;
    }
    drain_sink_into_cold_store(&sink)?;

    assert_v1_heatmap_capability(
        evidence,
        &client,
        &pyroscope_base,
        selector,
        fixture_timestamp,
    )
    .await?;

    compare_populated_span_profiles(
        evidence,
        &client,
        &pyroscope_base,
        &krabka.querier_base,
        selector,
        fixture_timestamp,
    )
    .await?;
    let analysis = compare_populated_query_analysis(
        evidence,
        &client,
        &pyroscope_base,
        &krabka.querier_base,
        selector,
        fixture_timestamp,
    )
    .await;
    if analysis.is_err() {
        krabka.shutdown();
        return analysis;
    }

    compare_populated_merge_profiles(
        evidence,
        &client,
        &pyroscope_base,
        &krabka.querier_base,
        fixture_timestamp,
    )
    .await?;

    let store = UnionProfileStore::new(Arc::new(store), Arc::new(sink.cold.clone()));

    compare_populated_diffs(
        evidence,
        &client,
        PopulatedBackends {
            oracle_base: &pyroscope_base,
            krabka_base: &krabka.querier_base,
            store: &store,
            fixture_timestamp,
        },
    )
    .await?;

    let generated = compare_generated_populated_profiles(
        evidence,
        &client,
        &pyroscope_base,
        &krabka.querier_base,
        &store,
        fixture_timestamp,
    )
    .await;
    krabka.shutdown();
    generated
}

// These numbers are independent fixture expectations, rather than values
// inferred from either backend. Pinned NewFlamegraphDiff preserves raw side
// totals and self values; Total adds both sides, MaxSelf takes their maximum.
// https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/pkg/model/flamegraph_diff.go
fn populated_diff_expected(reverse: bool) -> Value {
    let bars = if reverse {
        json!([
            [["total"], [28, 0, 140, 0]],
            [["total", FUNC_WORK], [28, 8, 140, 40]],
            [["total", FUNC_WORK, FUNC_HOT], [20, 20, 100, 100]],
        ])
    } else {
        json!([
            [["total"], [140, 0, 28, 0]],
            [["total", FUNC_WORK], [140, 40, 28, 8]],
            [["total", FUNC_WORK, FUNC_HOT], [100, 100, 20, 20]],
        ])
    };
    json!({"bars": bars, "total": 168, "maxSelf": 100,
        "leftTicks": if reverse {28} else {140},
        "rightTicks": if reverse {140} else {28}})
}

/// Serves a query-frontend over `store` on a loopback port, sharding each
/// query into 500ms ranges, and returns its base URL. It runs until the
/// returned sender fires or is dropped.
async fn start_sharded_frontend(
    store: &UnionProfileStore<WalTailProfileStore, WalTailProfileStore>,
) -> TestResult<(String, oneshot::Sender<()>)> {
    let (shutdown, shutdown_rx) = oneshot::channel();
    let frontend = query::serve(
        "127.0.0.1:0".parse()?,
        Arc::new(QuerierState::new_frontend(
            Arc::new(store.clone()),
            krabka_profiles::query_frontend::FrontendConfig {
                shard_width: krabka_units::millis(500),
            },
        )),
        &ServerSecurity::default(),
        async move {
            let _ = shutdown_rx.await;
        },
    )
    .await?;
    Ok((format!("http://{frontend}"), shutdown))
}

/// The two backends of a populated comparison, and the store and fixture
/// timestamp behind the Krabka one.
#[derive(Clone, Copy)]
struct PopulatedBackends<'a> {
    oracle_base: &'a str,
    krabka_base: &'a str,
    store: &'a UnionProfileStore<WalTailProfileStore, WalTailProfileStore>,
    fixture_timestamp: i64,
}

async fn compare_populated_diffs(
    evidence: &mut Value,
    client: &reqwest::Client,
    backends: PopulatedBackends<'_>,
) -> TestResult {
    use pb::querier::v1::{DiffRequest, DiffResponse, SelectMergeStacktracesRequest};

    let PopulatedBackends {
        oracle_base,
        krabka_base,
        store,
        fixture_timestamp,
    } = backends;
    let (frontend_base, shutdown) = start_sharded_frontend(store).await?;
    let side = |timestamp| SelectMergeStacktracesRequest {
        profile_type_id: CPU_PROFILE_TYPE.to_string(),
        label_selector: E2E_SELECTOR.to_string(),
        start: timestamp - 500,
        end: timestamp + 500,
        max_nodes: Some(1_024),
        ..Default::default()
    };
    let mut failures = Vec::new();
    for reverse in [false, true] {
        let name = if reverse {
            "unequal CPU reversed"
        } else {
            "unequal CPU"
        };
        let (left, right) = if reverse {
            (side(fixture_timestamp + 2_000), side(fixture_timestamp))
        } else {
            (side(fixture_timestamp), side(fixture_timestamp + 2_000))
        };
        let request = DiffRequest {
            left: Some(left),
            right: Some(right),
        };
        let expected = populated_diff_expected(reverse);
        evidence["active_case"] = json!({"method": "Diff", "name": name,
            "backend": "upstream", "phase": "fixture readiness", "request": request});
        write_profile_rpc_evidence(evidence)?;
        connect_json_until(
            client,
            oracle_base,
            None,
            "Diff",
            serde_json::to_value(&request)?,
            |value| canonical_diff(value).is_ok_and(|actual| actual == expected),
        )
        .await?;
        for (backend, base, tenant) in [
            ("upstream", oracle_base, None),
            ("krabka-direct", krabka_base, Some(TENANT)),
            ("krabka-frontend", frontend_base.as_str(), Some(TENANT)),
        ] {
            for transport in ["json", "protobuf"] {
                evidence["active_case"] = json!({"method": "Diff", "name": name,
                    "backend": backend, "transport": transport, "request": request});
                write_profile_rpc_evidence(evidence)?;
                let result: TestResult<Value> = async {
                    let value = if transport == "json" {
                        connect_json(
                            client,
                            base,
                            tenant,
                            "Diff",
                            serde_json::to_value(&request)?,
                        )
                        .await?
                    } else {
                        let response: DiffResponse =
                            connect_protobuf(client, base, tenant, "Diff", &request).await?;
                        serde_json::to_value(response)?
                    };
                    let actual = canonical_diff(&value)?;
                    if actual != expected {
                        return Err(format!(
                            "Diff {name}/{backend}/{transport}: expected {expected}, got {actual}"
                        )
                        .into());
                    }
                    Ok(actual)
                }
                .await;
                let error = result.as_ref().err().map(ToString::to_string);
                record_profile_rpc_case(
                    evidence,
                    json!({"method": "Diff", "name": name,
                    "backend": backend, "transport": transport, "request": request,
                    "classification": "matched", "expected": expected,
                    "actual": result.as_ref().ok(), "status": if result.is_ok() {"passed"} else {"failed"},
                    "error": error}),
                )?;
                if let Some(error) = error {
                    failures.push(error);
                }
            }
        }
    }
    let _ = shutdown.send(());
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n").into())
    }
}

async fn compare_populated_span_profiles(
    evidence: &mut Value,
    client: &reqwest::Client,
    oracle_base: &str,
    krabka_base: &str,
    selector: &str,
    fixture_timestamp: i64,
) -> TestResult {
    use pb::querier::v1::{SelectMergeSpanProfileRequest, SelectMergeSpanProfileResponse};

    let span_request = SelectMergeSpanProfileRequest {
        profile_type_id: OTLP_PROFILE_TYPE.to_string(),
        label_selector: selector.to_string(),
        span_selector: vec!["000000000000002a".to_string()],
        start: fixture_timestamp - 1_000,
        end: fixture_timestamp + 1_000,
        max_nodes: Some(1_024),
        ..Default::default()
    };
    connect_json_until(
        client,
        oracle_base,
        None,
        "SelectMergeSpanProfile",
        serde_json::to_value(&span_request)?,
        |value| value.pointer("/flamegraph/total").and_then(json_i64) == Some(100),
    )
    .await?;

    let hot = vec![(vec![FUNC_WORK.to_string(), FUNC_HOT.to_string()], 100)];
    let work = vec![(vec![FUNC_WORK.to_string()], 40)];
    let mut both = work.clone();
    both.extend(hot.clone());
    let mut cases = vec![
        ("hot span", span_request.clone(), hot.clone()),
        (
            "other span",
            SelectMergeSpanProfileRequest {
                span_selector: vec!["000000000000002b".to_string()],
                ..span_request.clone()
            },
            work,
        ),
        (
            "both spans",
            SelectMergeSpanProfileRequest {
                span_selector: vec![
                    "000000000000002a".to_string(),
                    "000000000000002b".to_string(),
                ],
                ..span_request.clone()
            },
            both,
        ),
        (
            "duplicate span selector",
            SelectMergeSpanProfileRequest {
                span_selector: vec!["000000000000002a".to_string(); 2],
                ..span_request.clone()
            },
            hot,
        ),
        (
            "missing span",
            SelectMergeSpanProfileRequest {
                span_selector: vec!["000000000000002c".to_string()],
                ..span_request.clone()
            },
            Vec::new(),
        ),
    ];
    cases.push((
        "excluded selector",
        SelectMergeSpanProfileRequest {
            label_selector: r#"{service_name="missing"}"#.to_string(),
            ..span_request.clone()
        },
        Vec::new(),
    ));
    cases.push((
        "excluded time",
        SelectMergeSpanProfileRequest {
            start: fixture_timestamp - 3_000,
            end: fixture_timestamp - 2_000,
            ..span_request.clone()
        },
        Vec::new(),
    ));
    cases.push((
        "excluded profile type",
        SelectMergeSpanProfileRequest {
            profile_type_id: WALL_PROFILE_TYPE.to_string(),
            ..span_request
        },
        Vec::new(),
    ));
    for (name, request, mut expected) in cases {
        expected.sort();
        for (base, tenant) in [(oracle_base, None), (krabka_base, Some(TENANT))] {
            evidence["active_case"] = json!({"method": "SelectMergeSpanProfile", "name": name,
                "backend": base, "request": request});
            let json: SelectMergeSpanProfileResponse = serde_json::from_value(
                connect_json(
                    client,
                    base,
                    tenant,
                    "SelectMergeSpanProfile",
                    serde_json::to_value(&request)?,
                )
                .await?,
            )?;
            let binary: SelectMergeSpanProfileResponse =
                connect_protobuf(client, base, tenant, "SelectMergeSpanProfile", &request).await?;
            for (transport, response) in [("json", json), ("protobuf", binary)] {
                let flamegraph = response
                    .flamegraph
                    .ok_or("span response missing flamegraph")?;
                let actual = normalized_flamegraph_stacks(&flamegraph)?;
                let total: i64 = expected.iter().map(|(_, value)| value).sum();
                record_profile_rpc_case(
                    evidence,
                    json!({
                        "method": "SelectMergeSpanProfile", "name": name, "backend": base,
                        "transport": transport, "classification": "matched",
                        "status": if actual == expected && flamegraph.total == total { "passed" } else { "failed" },
                        "expected_stacks": expected, "actual_stacks": actual,
                        "expected_total": total, "actual_total": flamegraph.total,
                    }),
                )?;
                assert2::assert!(actual == expected, "{name} from {base}");
                assert2::assert!(flamegraph.total == total, "{name} from {base}");
            }
        }
    }

    Ok(())
}

async fn compare_populated_query_analysis(
    evidence: &mut Value,
    client: &reqwest::Client,
    oracle_base: &str,
    krabka_base: &str,
    selector: &str,
    fixture_timestamp: i64,
) -> TestResult {
    use pb::querier::v1::{AnalyzeQueryRequest, AnalyzeQueryResponse};

    let mut analysis_failures = Vec::new();
    for (name, profile_type, selector, expected) in [
        ("matching", OTLP_PROFILE_TYPE, selector, 1),
        (
            "missing selector",
            OTLP_PROFILE_TYPE,
            r#"{service_name="missing"}"#,
            0,
        ),
        ("missing profile type", WALL_PROFILE_TYPE, selector, 0),
        ("selector-only matching", "", selector, 1),
        (
            "selector-only missing",
            "",
            r#"{service_name="missing"}"#,
            0,
        ),
        ("empty query", "", "", 3),
    ] {
        let request = AnalyzeQueryRequest {
            query: format!("{profile_type}{selector}"),
            start: fixture_timestamp - 1_000,
            end: fixture_timestamp + 1_000,
        };
        for (base, tenant) in [(oracle_base, None), (krabka_base, Some(TENANT))] {
            evidence["active_case"] = json!({"method": "AnalyzeQuery", "backend": base,
                "request": request, "classification": "matched"});
            let backend = RpcBackend {
                client,
                base,
                tenant,
            };
            for (transport, response) in
                analyze_query_over_both_transports(backend, &request).await?
            {
                if assert_queried_series_count(&response, expected).is_err()
                    && evidence.get("failure_diagnostics").is_none()
                {
                    let mut diagnostics = Vec::new();
                    for (method, body) in [
                        (
                            "Series",
                            json!({"matchers": [selector], "start": request.start, "end": request.end}),
                        ),
                        ("Series", json!({"matchers": [selector]})),
                        ("ProfileTypes", json!({})),
                        ("GetProfileStats", json!({})),
                        (
                            "AnalyzeQuery",
                            json!({"query": request.query, "start": 0, "end": i64::MAX}),
                        ),
                    ] {
                        let result = connect_json(client, base, tenant, method, body.clone()).await;
                        diagnostics.push(json!({"method": method, "request": body,
                            "response": result.as_ref().ok(), "error": result.err().map(|error| error.to_string())}));
                    }
                    evidence["failure_diagnostics"] = json!(diagnostics);
                }
                record_profile_rpc_case(
                    evidence,
                    with_queried_series_outcome(
                        json!({
                            "method": "AnalyzeQuery", "name": name, "backend": base, "transport": transport,
                            "request": request, "classification": "matched",
                        }),
                        &response,
                        expected,
                    ),
                )?;
                if let Err(error) = assert_queried_series_count(&response, expected) {
                    analysis_failures.push(format!("{error}: backend={base}, transport={transport}, request={request:?}, response={response:?}"));
                }
            }
        }
    }

    // The pinned public frontend returns an empty diagnostic whenever either
    // bound is omitted, before parsing the selector or planning storage.
    // pkg/frontend/frontend_analyze_query.go, not the downstream Series method.
    for (name, query, start, end) in [
        (
            "legacy omitted matching",
            format!("{OTLP_PROFILE_TYPE}{selector}"),
            0,
            0,
        ),
        (
            "legacy end omitted empty query",
            String::new(),
            fixture_timestamp + 1_000,
            0,
        ),
        (
            "legacy start omitted missing",
            format!("{OTLP_PROFILE_TYPE}{{service_name=\"missing\"}}"),
            0,
            fixture_timestamp + 1_000,
        ),
    ] {
        let request = AnalyzeQueryRequest { query, start, end };
        for (base, tenant) in [(oracle_base, None), (krabka_base, Some(TENANT))] {
            let backend = RpcBackend {
                client,
                base,
                tenant,
            };
            for (transport, response) in
                analyze_query_over_both_transports(backend, &request).await?
            {
                let expected = AnalyzeQueryResponse::default();
                let passed = response == expected;
                record_profile_rpc_case(
                    evidence,
                    json!({
                        "method":"AnalyzeQuery","name":name,"backend":base,"transport":transport,
                        "request":request,"classification":"matched","status":if passed {"passed"} else {"failed"},
                        "expected":expected,"response":response,"comparison_fields":["whole_empty_frontend_response_when_either_date_zero"],
                    }),
                )?;
                if !passed {
                    analysis_failures.push(format!("omitted-bound query analysis mismatch: backend={base},transport={transport},request={request:?},response={response:?}"));
                }
            }
        }
    }

    // AnalyzeQuery -> Series -> Head.Series ignores time bounds in pinned v1.
    // https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/pkg/phlaredb/head.go#L498-L532
    // Both backends must preserve this observable head-series behavior.
    let excluded_time = AnalyzeQueryRequest {
        query: format!("{OTLP_PROFILE_TYPE}{selector}"),
        start: fixture_timestamp - 3_000,
        end: fixture_timestamp - 2_000,
    };
    for (base, tenant, expected) in [(oracle_base, None, 1), (krabka_base, Some(TENANT), 1)] {
        evidence["active_case"] = json!({"method": "AnalyzeQuery", "backend": base,
            "request": excluded_time, "classification": "matched"});
        let backend = RpcBackend {
            client,
            base,
            tenant,
        };
        for (transport, response) in
            analyze_query_over_both_transports(backend, &excluded_time).await?
        {
            record_profile_rpc_case(
                evidence,
                with_queried_series_outcome(
                    json!({
                        "method": "AnalyzeQuery", "name":"excluded time", "backend": base, "transport": transport,
                        "request": excluded_time, "classification": "matched",
                        "reason": "pinned v1 Head.Series returns all selector-matching series within an overlapping head without per-profile time filtering",
                        "source": "https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/pkg/phlaredb/head.go#L498-L532",
                    }),
                    &response,
                    expected,
                ),
            )?;
            if let Err(error) = assert_queried_series_count(&response, expected) {
                analysis_failures.push(format!("{error}: backend={base}, transport={transport}, v1 head-series compatibility, request={excluded_time:?}, response={response:?}"));
            }
        }
    }

    if !analysis_failures.is_empty() {
        return Err(analysis_failures.join("\n").into());
    }

    Ok(())
}

async fn compare_generated_populated_profiles(
    evidence: &mut Value,
    client: &reqwest::Client,
    oracle_base: &str,
    krabka_base: &str,
    store: &UnionProfileStore<WalTailProfileStore, WalTailProfileStore>,
    fixture_timestamp: i64,
) -> TestResult {
    let cpu_request = pb::querier::v1::SelectMergeStacktracesRequest {
        profile_type_id: CPU_PROFILE_TYPE.to_string(),
        label_selector: E2E_SELECTOR.to_string(),
        start: fixture_timestamp - 1_000,
        end: fixture_timestamp + 1_000,
        max_nodes: Some(1_024),
        format: pb::querier::v1::ProfileFormat::Flamegraph as i32,
        ..Default::default()
    };
    let expected_cpu_stacks = vec![
        (vec![FUNC_WORK.to_string()], 40),
        (vec![FUNC_WORK.to_string(), FUNC_HOT.to_string()], 100),
    ];
    evidence["active_case"] =
        json!({"method": "SelectMergeStacktraces", "phase": "generated-positive-control"});
    // Independently establish the populated CPU fixture before composing
    // selectors. Equality between two empty responses is not sufficient.
    if let Some(mismatch) = compare_generated_profile_selector(
        client,
        oracle_base,
        krabka_base,
        &cpu_request,
        &expected_cpu_stacks,
    )
    .await?
    {
        return Err(format!("generated profile positive control: {mismatch}").into());
    }
    let output = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR").map_or_else(
        || std::env::temp_dir().join(format!("krabka-profile-generated-{}", std::process::id())),
        std::path::PathBuf::from,
    );
    evidence["active_case"] =
        json!({"method": "SelectMergeStacktraces", "phase": "generated-compound-selectors"});
    evidence["generated_evidence"] = json!("pyroscope-generated-differential.json");
    let expected = &expected_cpu_stacks;
    generated_differential::run_typed(
        "pyroscope",
        &[TypedExpr::profile_selector(&[LabelMatcher::new(
            "service_name",
            MatchOp::Eq,
            "checkout",
        )])],
        &[
            TypedConstructor::ProfileAnd(LabelMatcher::new("env", MatchOp::Eq, "e2e")),
            TypedConstructor::ProfileAnd(LabelMatcher::new("env", MatchOp::Neq, "missing")),
            TypedConstructor::ProfileAnd(LabelMatcher::new("env", MatchOp::Regex, "e.*")),
            TypedConstructor::ProfileAnd(LabelMatcher::new("missing", MatchOp::NotRegex, ".+")),
        ],
        &output,
        |expression| {
            let mut request = cpu_request.clone();
            request.label_selector = format!("{{{expression}}}");
            async move {
                compare_generated_profile_selector(
                    client,
                    oracle_base,
                    krabka_base,
                    &request,
                    expected,
                )
                .await
            }
        },
    )
    .await?;
    evidence["active_case"] =
        json!({"method": "SelectMergeSpanProfile", "phase": "generated-rejections"});
    evidence["generated_rejection_evidence"] =
        json!("pyroscope-rejections-generated-differential.json");
    write_profile_rpc_evidence(evidence)?;
    let rejection_result = run_generated_profile_rejections(
        client,
        PopulatedBackends {
            oracle_base,
            krabka_base,
            store,
            fixture_timestamp,
        },
        &output,
    )
    .await;
    rejection_result?;
    Ok(())
}

async fn run_generated_profile_rejections(
    client: &reqwest::Client,
    backends: PopulatedBackends<'_>,
    output: &std::path::Path,
) -> TestResult {
    let PopulatedBackends {
        oracle_base,
        krabka_base,
        store,
        fixture_timestamp: time_ms,
    } = backends;
    let (frontend_base, shutdown) = start_sharded_frontend(store).await?;
    let frontend_base = &frontend_base;
    let result = generated_differential::run(
        "pyroscope-rejections",
        &[
            r#"service_name="krabkadiff-linked",env="#,
            r#"service_name=~"[""#,
            r#"service_name="krabkadiff-linked",env=~"(""#,
        ],
        &[
            r#"{expr},env="e2e""#,
            r#"{expr},zone!="missing""#,
            r#"{expr},zone=~"test.*""#,
        ],
        output,
        |expression| async move {
            let request = pb::querier::v1::SelectMergeSpanProfileRequest {
                profile_type_id: OTLP_PROFILE_TYPE.to_string(),
                label_selector: format!("{{{expression}}}"),
                span_selector: vec!["000000000000002a".to_string()],
                start: time_ms - 1_000,
                end: time_ms + 1_000,
                max_nodes: Some(1_024),
                ..Default::default()
            };
            let mut mismatches = Vec::new();
            // Establish invalidity with upstream first, then check both
            // Krabka execution paths. Preserve error replies as evidence;
            // successful bodies or paired unrelated errors cannot pass.
            for (backend, base, tenant) in [
                ("upstream", oracle_base, None),
                ("krabka-direct", krabka_base, Some(TENANT)),
                ("krabka-frontend", frontend_base, Some(TENANT)),
            ] {
                let mut call = client
                    .post(format!("{base}/querier.v1.QuerierService/SelectMergeSpanProfile"))
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .json(&request);
                if let Some(tenant) = tenant {
                    call = call.header("x-scope-orgid", tenant);
                }
                let response = call.send().await?;
                let status = response.status();
                let body = response.text().await?;
                let error = serde_json::from_str::<Value>(&body).ok();
                let code = error.as_ref().and_then(|error| error.get("code")).and_then(Value::as_str);
                if status != StatusCode::BAD_REQUEST || code != Some("invalid_argument") {
                    mismatches.push(format!(
                        "{backend} selector `{}`: expected HTTP400/invalid_argument, got {status}/{code:?}, body={body}",
                        request.label_selector
                    ));
                    if backend == "upstream" {
                        return Ok(Some(mismatches.join("\n")));
                    }
                }
            }
            Ok((!mismatches.is_empty()).then(|| mismatches.join("\n")))
        },
    )
    .await;
    let _ = shutdown.send(());
    result
}

async fn compare_populated_merge_profiles(
    evidence: &mut Value,
    client: &reqwest::Client,
    oracle_base: &str,
    krabka_base: &str,
    time_ms: i64,
) -> TestResult {
    use pb::{
        google::v1::Profile,
        querier::v1::SelectMergeProfileRequest,
        types::v1::{Location, StackTraceSelector},
    };

    let request = SelectMergeProfileRequest {
        profile_type_id: CPU_PROFILE_TYPE.to_string(),
        label_selector: E2E_SELECTOR.to_string(),
        start: time_ms - 1_000,
        end: time_ms + 1_000,
        max_nodes: Some(1_024),
        ..Default::default()
    };
    let expected = vec![
        (vec![FUNC_WORK.to_string()], 40),
        (vec![FUNC_WORK.to_string(), FUNC_HOT.to_string()], 100),
    ];
    evidence["active_case"] = json!({"method": "SelectMergeProfile", "name": "fixture readiness"});
    connect_json_until(
        client,
        oracle_base,
        None,
        "SelectMergeProfile",
        serde_json::to_value(&request)?,
        |value| {
            serde_json::from_value::<Profile>(value.clone())
                .ok()
                .and_then(|profile| normalized_pprof_stacks(&profile).ok())
                .is_some_and(|stacks| stacks == expected)
        },
    )
    .await?;

    // Pinned types.proto defines call_site as a root-first stack prefix.
    // https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/api/types/v1/types.proto
    let stack_selector = |names: &[&str]| {
        Some(StackTraceSelector {
            call_site: names
                .iter()
                .map(|name| Location {
                    name: (*name).to_string(),
                })
                .collect(),
            ..Default::default()
        })
    };
    let cases = [
        ("populated CPU", request.clone(), expected.clone()),
        (
            "excluded selector",
            SelectMergeProfileRequest {
                label_selector: r#"{service_name="missing"}"#.to_string(),
                ..request.clone()
            },
            Vec::new(),
        ),
        (
            "excluded time",
            SelectMergeProfileRequest {
                start: time_ms - 3_000,
                end: time_ms - 2_000,
                ..request.clone()
            },
            Vec::new(),
        ),
        (
            "excluded profile type",
            SelectMergeProfileRequest {
                profile_type_id: WALL_PROFILE_TYPE.to_string(),
                ..request.clone()
            },
            Vec::new(),
        ),
        (
            "root stack prefix",
            SelectMergeProfileRequest {
                stack_trace_selector: stack_selector(&[FUNC_WORK]),
                ..request.clone()
            },
            expected,
        ),
        (
            "hot stack prefix",
            SelectMergeProfileRequest {
                stack_trace_selector: stack_selector(&[FUNC_WORK, FUNC_HOT]),
                ..request.clone()
            },
            vec![(vec![FUNC_WORK.to_string(), FUNC_HOT.to_string()], 100)],
        ),
        (
            "missing stack prefix",
            SelectMergeProfileRequest {
                stack_trace_selector: stack_selector(&[FUNC_WORK, "missing"]),
                ..request
            },
            Vec::new(),
        ),
    ];
    for (name, request, expected) in cases {
        for (base, tenant) in [(oracle_base, None), (krabka_base, Some(TENANT))] {
            evidence["active_case"] = json!({"method": "SelectMergeProfile", "name": name,
                "backend": base, "request": request});
            let json: Profile = serde_json::from_value(
                connect_json(
                    client,
                    base,
                    tenant,
                    "SelectMergeProfile",
                    serde_json::to_value(&request)?,
                )
                .await?,
            )?;
            let binary: Profile =
                connect_protobuf(client, base, tenant, "SelectMergeProfile", &request).await?;
            for (transport, profile) in [("json", json), ("protobuf", binary)] {
                let normalized = normalized_pprof_stacks(&profile);
                let expected_total: i64 = expected.iter().map(|(_, value)| value).sum();
                let actual_total: i64 =
                    profile.sample.iter().flat_map(|sample| &sample.value).sum();
                let matched = normalized.as_ref().is_ok_and(|stacks| stacks == &expected)
                    && actual_total == expected_total;
                record_profile_rpc_case(
                    evidence,
                    json!({"method": "SelectMergeProfile", "name": name, "backend": base,
                        "transport": transport, "request": request, "classification": "matched",
                        "status": if matched { "passed" } else { "failed" },
                        "expected_stacks": expected, "actual_stacks": normalized.as_ref().ok(),
                        "normalization_error": normalized.as_ref().err().map(ToString::to_string),
                        "expected_total": expected_total, "actual_total": actual_total}),
                )?;
                if !matched {
                    return Err(format!(
                        "SelectMergeProfile {name}, {base}, {transport}: expected {expected:?}, total {expected_total}; actual {normalized:?}, total {actual_total}"
                    ).into());
                }
            }
        }
    }
    Ok(())
}

/// Resolve each pprof sample's leaf-first location/function/string references
/// into a root-first stack and its value. Preserve repeated stacks and check
/// the single CPU sample type so totals cannot mask wrong symbol associations.
fn normalized_pprof_stacks(
    profile: &pb::google::v1::Profile,
) -> TestResult<Vec<(Vec<String>, i64)>> {
    let string = |index: i64| -> TestResult<&str> {
        usize::try_from(index)
            .ok()
            .and_then(|index| profile.string_table.get(index))
            .map(String::as_str)
            .ok_or_else(|| format!("pprof string index out of bounds: {index}").into())
    };
    if !profile.sample.is_empty()
        && (profile.sample_type.len() != 1
            || string(profile.sample_type[0].r#type)? != "cpu"
            || string(profile.sample_type[0].unit)? != "nanoseconds")
    {
        return Err("pprof sample type is not CPU nanoseconds".into());
    }
    let location_ids: BTreeSet<_> = profile
        .location
        .iter()
        .map(|location| location.id)
        .collect();
    let function_ids: BTreeSet<_> = profile
        .function
        .iter()
        .map(|function| function.id)
        .collect();
    if location_ids.len() != profile.location.len()
        || function_ids.len() != profile.function.len()
        || location_ids.contains(&0)
        || function_ids.contains(&0)
    {
        return Err("pprof has duplicate or zero location/function IDs".into());
    }
    let mut stacks = Vec::new();
    for sample in &profile.sample {
        let [value] = sample.value.as_slice() else {
            return Err("pprof sample must contain exactly one CPU value".into());
        };
        if sample.location_id.is_empty() {
            return Err("pprof sample has no stack".into());
        }
        let mut stack = Vec::new();
        for id in sample.location_id.iter().rev() {
            let location = profile
                .location
                .iter()
                .find(|location| location.id == *id)
                .ok_or_else(|| format!("pprof missing sample location {id}"))?;
            if location.line.is_empty() {
                return Err("pprof CPU fixture location has no symbolized lines".into());
            }
            for line in location.line.iter().rev() {
                let function = profile
                    .function
                    .iter()
                    .find(|function| function.id == line.function_id)
                    .ok_or_else(|| format!("pprof missing line function {}", line.function_id))?;
                let name = string(function.name)?;
                if name.is_empty() {
                    return Err("pprof CPU fixture has an empty function name".into());
                }
                stack.push(name.to_string());
            }
        }
        stacks.push((stack, *value));
    }
    stacks.sort();
    Ok(stacks)
}

async fn compare_generated_profile_selector(
    client: &reqwest::Client,
    oracle_base: &str,
    krabka_base: &str,
    request: &pb::querier::v1::SelectMergeStacktracesRequest,
    expected: &[(Vec<String>, i64)],
) -> TestResult<Option<String>> {
    for (base, tenant) in [(oracle_base, None), (krabka_base, Some(TENANT))] {
        let json: pb::querier::v1::SelectMergeStacktracesResponse = serde_json::from_value(
            connect_json(
                client,
                base,
                tenant,
                "SelectMergeStacktraces",
                serde_json::to_value(request)?,
            )
            .await?,
        )?;
        let binary: pb::querier::v1::SelectMergeStacktracesResponse =
            connect_protobuf(client, base, tenant, "SelectMergeStacktraces", request).await?;
        for (transport, response) in [("json", json), ("protobuf", binary)] {
            let graph = response
                .flamegraph
                .ok_or("generated profile response missing flamegraph")?;
            let actual = normalized_flamegraph_stacks(&graph)?;
            let expected_total: i64 = expected.iter().map(|(_, value)| value).sum();
            if actual != expected || graph.total != expected_total || graph.max_self != 100 {
                return Ok(Some(format!(
                    "selector={} backend={base} transport={transport}: expected stacks={expected:?}, total={expected_total}, maxSelf=100; actual stacks={actual:?}, total={}, maxSelf={}",
                    request.label_selector, graph.total, graph.max_self
                )));
            }
        }
    }
    Ok(None)
}

async fn assert_v1_heatmap_capability(
    evidence: &mut Value,
    client: &reqwest::Client,
    pyroscope_base: &str,
    selector: &str,
    time_ms: i64,
) -> TestResult {
    evidence["active_case"] =
        json!({"method": "SelectHeatmap", "classification": "unsupported-oracle"});
    let response = client
        .post(format!(
            "{pyroscope_base}/querier.v1.QuerierService/SelectHeatmap"
        ))
        .json(
            &json!({"profileTypeID": OTLP_PROFILE_TYPE, "labelSelector": selector,
            "start": time_ms - 1_000, "end": time_ms + 1_000, "step": 1.0}),
        )
        .send()
        .await?;
    let status = response.status();
    let error: Value = response.json().await?;
    evidence["oracle_capabilities"]["SelectHeatmap"]["status"] = json!(if status
        == StatusCode::NOT_IMPLEMENTED
        && error.get("code").and_then(Value::as_str) == Some("unimplemented")
    {
        "confirmed-unimplemented"
    } else {
        "failed"
    });
    record_profile_rpc_case(
        evidence,
        json!({
            "method": "SelectHeatmap", "transport": "json", "classification": "unsupported-oracle",
            "semantic_comparison": false, "expected_http_status": 501, "actual_http_status": status.as_u16(),
            "actual_code": error.get("code"),
            "status": if status == StatusCode::NOT_IMPLEMENTED && error.get("code").and_then(Value::as_str) == Some("unimplemented") { "confirmed-unimplemented" } else { "failed" },
        }),
    )?;
    assert2::assert!(status == StatusCode::NOT_IMPLEMENTED);
    assert2::assert!(error.get("code").and_then(Value::as_str) == Some("unimplemented"));
    Ok(())
}

/// One backend a querier RPC is sent to: its base URL, and the tenant header
/// Krabka needs and the single-tenant oracle does not.
#[derive(Clone, Copy)]
struct RpcBackend<'a> {
    client: &'a reqwest::Client,
    base: &'a str,
    tenant: Option<&'a str>,
}

/// `request`'s `AnalyzeQuery` answer from `backend`, once over JSON and once
/// over protobuf, each named by its transport.
async fn analyze_query_over_both_transports(
    backend: RpcBackend<'_>,
    request: &pb::querier::v1::AnalyzeQueryRequest,
) -> TestResult<[(&'static str, pb::querier::v1::AnalyzeQueryResponse); 2]> {
    let RpcBackend {
        client,
        base,
        tenant,
    } = backend;
    let json: pb::querier::v1::AnalyzeQueryResponse = serde_json::from_value(
        connect_json(
            client,
            base,
            tenant,
            "AnalyzeQuery",
            serde_json::to_value(request)?,
        )
        .await?,
    )?;
    let binary: pb::querier::v1::AnalyzeQueryResponse =
        connect_protobuf(client, base, tenant, "AnalyzeQuery", request).await?;
    Ok([("json", json), ("protobuf", binary)])
}

/// Orders exemplars, and each exemplar's labels, canonically, so two backends
/// that return the same exemplars in different orders compare equal.
fn sort_exemplars(exemplars: &mut [pb::querier::v1::Exemplar]) {
    for exemplar in exemplars.iter_mut() {
        exemplar
            .labels
            .sort_by(|a, b| (&a.name, &a.value).cmp(&(&b.name, &b.value)));
    }
    exemplars.sort_by_key(|e| {
        (
            e.timestamp,
            e.value,
            e.profile_id.clone(),
            e.trace_id.clone(),
            e.span_id.clone(),
        )
    });
}

async fn connect_protobuf<Req: prost::Message, Resp: prost::Message + Default>(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    method: &str,
    request: &Req,
) -> TestResult<Resp> {
    let mut request = client
        .post(format!("{base}/querier.v1.QuerierService/{method}"))
        .header(reqwest::header::CONTENT_TYPE, "application/proto")
        .body(request.encode_to_vec());
    if let Some(tenant) = tenant {
        request = request.header("x-scope-orgid", tenant);
    }
    let response = request.send().await?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let bytes = response.bytes().await?;
    if !status.is_success() {
        return Err(format!(
            "{method} returned {status}: {}",
            String::from_utf8_lossy(&bytes)
        )
        .into());
    }
    assert2::assert!(content_type.starts_with("application/proto"));
    Ok(Resp::decode(bytes)?)
}

/// Decode delta-encoded bars into stacks, preserving duplicate stack entries
/// and their values. Only sibling layout and name-table order are irrelevant.
fn normalized_flamegraph_stacks(
    graph: &pb::querier::v1::FlameGraph,
) -> TestResult<Vec<(Vec<String>, i64)>> {
    let mut parents: Vec<(i64, i64, Vec<String>)> = Vec::new();
    let mut stacks = Vec::new();
    for (depth, level) in graph.levels.iter().enumerate() {
        let (bars, remainder) = level.values.as_chunks::<4>();
        if !remainder.is_empty() {
            return Err("malformed flamegraph bar".into());
        }
        let mut current = Vec::new();
        let mut x = 0_i64;
        for bar in bars {
            x = x.checked_add(bar[0]).ok_or("flamegraph offset overflow")?;
            let end = x.checked_add(bar[1]).ok_or("flamegraph extent overflow")?;
            let name = usize::try_from(bar[3])
                .ok()
                .and_then(|index| graph.names.get(index))
                .ok_or("flamegraph name index out of bounds")?;
            let mut path = if depth == 0 {
                Vec::new()
            } else {
                parents
                    .iter()
                    .find(|(left, right, _)| *left <= x && end <= *right)
                    .ok_or("flamegraph bar has no parent")?
                    .2
                    .clone()
            };
            if depth != 0 {
                path.push(name.clone());
            }
            if bar[2] != 0 {
                stacks.push((path.clone(), bar[2]));
            }
            current.push((x, end, path));
            x = end;
        }
        parents = current;
    }
    stacks.sort();
    Ok(stacks)
}

/// `case` with the outcome of an `AnalyzeQuery` series-count comparison
/// appended after its own fields: whether `response` queried `expected`
/// series, both counts, the response, and the field compared.
fn with_queried_series_outcome(
    mut case: Value,
    response: &pb::querier::v1::AnalyzeQueryResponse,
    expected: u64,
) -> Value {
    case["status"] = json!(if assert_queried_series_count(response, expected).is_ok() {
        "passed"
    } else {
        "failed"
    });
    case["expected_count"] = json!(expected);
    case["actual_count"] = json!(
        response
            .query_impact
            .as_ref()
            .map(|impact| impact.total_queried_series)
    );
    case["response"] = json!(response);
    case["comparison_fields"] = json!(["query_impact.total_queried_series"]);
    case
}

fn assert_queried_series_count(
    response: &pb::querier::v1::AnalyzeQueryResponse,
    expected: u64,
) -> TestResult {
    let actual = response
        .query_impact
        .as_ref()
        .ok_or("AnalyzeQuery missing queryImpact")?
        .total_queried_series;
    if actual != expected {
        return Err(format!("AnalyzeQuery series count: expected {expected}, got {actual}").into());
    }
    Ok(())
}

#[test]
fn select_series_fixture_uses_shared_timestamp_and_preserves_profile() -> TestResult {
    use std::io::Read;

    let original = synthetic_cpu_pprof(0)?;
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(original.as_slice()).read_to_end(&mut decoded)?;
    let mut expected: proto::Profile = PprofProfile::decode(&decoded)?.into();
    expected.time_nanos = 100_000_000_000;
    let stamped = timestamp_goroutine_profile(&original, expected.time_nanos)?;
    decoded.clear();
    flate2::read::GzDecoder::new(stamped.as_slice()).read_to_end(&mut decoded)?;
    let actual: proto::Profile = PprofProfile::decode(&decoded)?.into();
    assert2::assert!(actual == expected);
    assert2::assert!(
        select_series_body(actual.time_nanos / 1_000_000)
            == json!({
                "profileTypeID": PROFILE_TYPE,
                "labelSelector": SELECTOR,
                "start": 80_123,
                "end": 120_000,
                "groupBy": ["env"],
                "step": 10.0,
                "aggregation": "TIME_SERIES_AGGREGATION_TYPE_SUM",
                "limit": 10,
            })
    );
    Ok(())
}

#[test]
fn profile_semantic_comparators_reject_index_timestamp_and_count_drift() -> TestResult {
    let expected = json!({"flamegraph": {"names": ["total", "work", "hot"],
        "levels": [{"values": [0, 140, 0, 0]}, {"values": [0, 140, 40, 1]},
            {"values": [0, 100, 100, 2]}], "total": 140, "maxSelf": 100}});
    let mut swapped_names = expected.clone();
    swapped_names["flamegraph"]["names"] = json!(["total", "hot", "work"]);
    assert2::assert!(
        assert_connect_flamegraph_equal("SelectMergeSpanProfile", &expected, &swapped_names)
            .is_err()
    );
    let mut swapped_values = expected.clone();
    // Same values as the oracle, associated with different bar fields.
    swapped_values["flamegraph"]["levels"][1]["values"] = json!([0, 40, 140, 1]);
    assert2::assert!(
        assert_connect_flamegraph_equal("SelectMergeSpanProfile", &expected, &swapped_values)
            .is_err()
    );
    let series = json!({"series": [{"labels": [], "points": [{"timestamp": 10, "value": 7.0}]}]});
    let mut shifted = series.clone();
    shifted["series"][0]["points"][0]["timestamp"] = json!(11);
    assert2::assert!(assert_select_series_equal(&series, &shifted).is_err());
    let analysis = pb::querier::v1::AnalyzeQueryResponse {
        query_impact: Some(pb::querier::v1::QueryImpact {
            total_queried_series: 2,
            ..Default::default()
        }),
        ..Default::default()
    };
    assert2::assert!(assert_queried_series_count(&analysis, 1).is_err());
    assert2::assert!(
        assert_queried_series_count(&pb::querier::v1::AnalyzeQueryResponse::default(), 0).is_err()
    );
    let graph: pb::querier::v1::FlameGraph =
        serde_json::from_value(expected["flamegraph"].clone())?;
    assert2::assert!(
        normalized_flamegraph_stacks(&graph)?
            == vec![
                (vec!["work".to_string()], 40),
                (vec!["work".to_string(), "hot".to_string()], 100),
            ]
    );
    let mut duplicate = graph.clone();
    duplicate.levels[2].values.extend([0, 100, 100, 2]);
    assert2::assert!(normalized_flamegraph_stacks(&duplicate).is_err());
    let mut repeated_stack = graph.clone();
    repeated_stack.levels[2].values = vec![0, 50, 50, 2, 0, 50, 50, 2];
    let repeated = normalized_flamegraph_stacks(&repeated_stack)?;
    assert2::assert!(repeated.len() == 3);
    assert2::assert!(repeated != normalized_flamegraph_stacks(&graph)?);
    Ok(())
}

#[test]
fn pprof_stack_comparator_rejects_value_and_symbol_association_drift() -> TestResult {
    use std::io::Read;

    use prost::Message;

    let compressed = synthetic_cpu_pprof(123)?;
    let mut bytes = Vec::new();
    flate2::read::GzDecoder::new(compressed.as_slice()).read_to_end(&mut bytes)?;
    let profile = pb::google::v1::Profile::decode(bytes.as_slice())?;
    let expected = vec![
        (vec![FUNC_WORK.to_string()], 40),
        (vec![FUNC_WORK.to_string(), FUNC_HOT.to_string()], 100),
    ];
    assert2::assert!(normalized_pprof_stacks(&profile)? == expected);
    let mut swapped_values = profile.clone();
    swapped_values.sample[0].value[0] = 40;
    swapped_values.sample[1].value[0] = 100;
    // Totals and the set of function names still agree; their association
    // with sample values is wrong and must fail the independent expectation.
    assert2::assert!(normalized_pprof_stacks(&swapped_values)? != expected);
    let mut swapped_symbols = profile.clone();
    swapped_symbols.function[0].name = 4;
    swapped_symbols.function[1].name = 3;
    assert2::assert!(normalized_pprof_stacks(&swapped_symbols)? != expected);
    let mut missing_location = profile.clone();
    missing_location.sample[0].location_id[0] = 999;
    assert2::assert!(normalized_pprof_stacks(&missing_location).is_err());
    let mut duplicate_id = profile.clone();
    duplicate_id.location[1].id = duplicate_id.location[0].id;
    assert2::assert!(normalized_pprof_stacks(&duplicate_id).is_err());
    let mut wrong_unit = profile.clone();
    wrong_unit.sample_type[0].unit = 3;
    assert2::assert!(normalized_pprof_stacks(&wrong_unit).is_err());
    let mut malformed_value = profile;
    malformed_value.sample[0].value.push(1);
    assert2::assert!(normalized_pprof_stacks(&malformed_value).is_err());
    Ok(())
}

const OTLP_JSON_SERVICE: &str = "krabkadiffotlp-json";
const OTLP_GZIP_SERVICE: &str = "krabkadiffotlp-gzip";
const OTLP_PROFILE_TYPE: &str = "cpu:cpu:nanoseconds:cpu:nanoseconds";

/// The same two-frame CPU profile the other cases use, in OTLP's table form:
/// one stack per sample, every symbol reached through the shared dictionary.
fn otlp_export_body(time_unix_nano: u64, service: &str, json: bool) -> TestResult<Vec<u8>> {
    use krabka_profiles::wire::pb::{
        opentelemetry::proto::{
            common::v1::{AnyValue, KeyValue, any_value::Value as AnyValueValue},
            resource::v1::Resource,
        },
        otlp_profiles::{
            ExportProfilesServiceRequest, Function, Line, Location, Mapping, Profile,
            ProfilesDictionary, ResourceProfiles, Sample, ScopeProfiles, Stack, ValueType,
        },
    };

    // 0="" 1="cpu" 2="nanoseconds" 3=main.work 4=main.hotloop 5="app.go"
    let dictionary = ProfilesDictionary {
        string_table: ["", "cpu", "nanoseconds", FUNC_WORK, FUNC_HOT, "app.go"]
            .into_iter()
            .map(String::from)
            .collect(),
        // Pyroscope refuses an export whose locations point at a mapping that
        // is not there, so the one mapping every location shares is declared
        // even though the profile is fully symbolized.
        mapping_table: vec![Mapping {
            filename_strindex: 5,
            ..Mapping::default()
        }],
        function_table: vec![
            Function {
                name_strindex: 3,
                system_name_strindex: 3,
                filename_strindex: 5,
                start_line: 1,
            },
            Function {
                name_strindex: 4,
                system_name_strindex: 4,
                filename_strindex: 5,
                start_line: 2,
            },
        ],
        location_table: vec![
            Location {
                address: 0x1000,
                lines: vec![Line {
                    function_index: 0,
                    line: 10,
                    column: 0,
                }],
                ..Location::default()
            },
            Location {
                address: 0x2000,
                lines: vec![Line {
                    function_index: 1,
                    line: 20,
                    column: 0,
                }],
                ..Location::default()
            },
        ],
        stack_table: vec![
            // Leaf-first, as in pprof: main.hotloop called from main.work.
            Stack {
                location_indices: vec![1, 0],
            },
            Stack {
                location_indices: vec![0],
            },
        ],
        ..ProfilesDictionary::default()
    };

    let profile = Profile {
        sample_type: Some(ValueType {
            type_strindex: 1,
            unit_strindex: 2,
        }),
        period_type: Some(ValueType {
            type_strindex: 1,
            unit_strindex: 2,
        }),
        period: 10_000_000,
        time_unix_nano,
        duration_nano: 1_000_000_000,
        samples: vec![
            Sample {
                stack_index: 0,
                values: vec![100],
                ..Sample::default()
            },
            Sample {
                stack_index: 1,
                values: vec![40],
                ..Sample::default()
            },
        ],
        ..Profile::default()
    };

    let request = ExportProfilesServiceRequest {
        resource_profiles: vec![ResourceProfiles {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueValue::StringValue(service.to_string())),
                    }),
                }],
                ..Resource::default()
            }),
            scope_profiles: vec![ScopeProfiles {
                profiles: vec![profile],
                ..ScopeProfiles::default()
            }],
            ..ResourceProfiles::default()
        }],
        dictionary: Some(dictionary),
    };
    if json {
        Ok(serde_json::to_vec(&request)?)
    } else {
        Ok(prost::Message::encode_to_vec(&request))
    }
}

async fn post_otlp_export(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    body: &[u8],
    content_type: &str,
    content_encoding: Option<&str>,
) -> TestResult {
    let mut request = client
        .post(format!("{base}/v1development/profiles"))
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(body.to_vec());
    if let Some(content_encoding) = content_encoding {
        request = request.header(reqwest::header::CONTENT_ENCODING, content_encoding);
    }
    send_expecting_ok(
        request,
        &ExpectedOk {
            tenant,
            what: &format!("OTLP export to {base}"),
        },
    )
    .await
}

/// The modern RPC fields require v2; a successful v1 capability probe is not
/// semantic evidence for heatmaps, linked samples or exemplars.
#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/pyroscope image"]
async fn real_pyroscope_v2_request_fields_match_krabka() -> TestResult {
    let client = reqwest::Client::new();
    let oracle = start_pyroscope_with_options(&[
        "-architecture.storage=v2",
        "-write-path=segment-writer",
        "-self-profiling.disable-push=true",
        "-query-frontend.async-queries-enabled=true",
    ])
    .await?;
    let oracle_base = mapped_base_url(&oracle, PYROSCOPE_HTTP_PORT).await?;
    wait_for_http_ok(&client, &oracle_base, &["/ready"]).await?;
    let sink = CapturingSink::default();
    let store = WalTailProfileStore::new();
    let candidate = start_krabka_pair_with_architecture(
        sink.clone(),
        store.clone(),
        query::PyroscopeQueryArchitecture::V2,
    )
    .await?;
    let time = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())? - 60_000;
    let (profile_id, identity) =
        profile_v2_fixtures(&client, &oracle_base, &candidate, &sink, time).await?;

    let selector = r#"{service_name="v2-matrix"}"#;
    let base = json!({"profileTypeID": OTLP_PROFILE_TYPE, "labelSelector": selector, "start": time-1000, "end": time+1000});
    connect_json_until(
        &client,
        &oracle_base,
        None,
        "SelectMergeStacktraces",
        base.clone(),
        |value| value["flamegraph"]["total"].as_str() == Some("140"),
    )
    .await?;
    connect_json_until(&client,&oracle_base,None,"SelectMergeStacktraces",json!({"profileTypeID":OTLP_PROFILE_TYPE,"labelSelector":"{service_name=\"v2-matrix-other\"}","start":time-1000,"end":time+1000}),|value|value["flamegraph"]["total"].as_str()==Some("50")).await?;
    let cases = profile_v2_request_cases(&base, time, &profile_id, selector)?;
    let fixture_hash = hex::encode(Sha256::digest(synthetic_cpu_pprof(time * 1_000_000)?));
    let mut evidence = json!({"fixture":{"cpu_pprof_sha256":fixture_hash,"primary_stacks":[[[FUNC_WORK],40],[[FUNC_WORK,FUNC_HOT],100]],"secondary_stacks":[[[FUNC_WORK],15],[[FUNC_WORK,FUNC_HOT],35]],"primary_total":140,"secondary_total":50},"suite":"pyroscope-v2-fields", "upstream_revision":"7aeaa0ff91e83538b3ff0d09bfefb168bddc022d", "storage":"v2", "upstream":{"image_tag":std::env::var("KRABKA_PYROSCOPE_IMAGE_TAG").ok(),"image_id":std::env::var("KRABKA_PYROSCOPE_IMAGE_ID").ok()}, "settings":{"oracle":{"query-frontend.async-queries-enabled":true},"candidate":{"query_architecture":"v2","async_queries_enabled":true}}, "identity_witness":{"requested":"03030303-0303-0303-0303-030303030303","upstream_assigned":profile_id,"candidate_stored":profile_id,"oracle_response":identity}, "planned":cases.len()+4, "status":"running", "cases":[]});
    let mut failures = Vec::new();
    for (method, name, request) in cases {
        let oracle =
            profile_v2_response(&client, &oracle_base, None, method, request.clone()).await;
        let candidate = profile_v2_response(
            &client,
            &candidate.querier_base,
            Some(TENANT),
            method,
            request.clone(),
        )
        .await;
        let (independent_expected, independent) =
            profile_v2_independent_witness(method, &name, time, &oracle, &candidate);
        let matched = independent
            && match (&oracle, &candidate) {
                (Ok(a), Ok(b)) => a == b,
                _ => false,
            };
        evidence["cases"].as_array_mut().ok_or("evidence cases missing")?.push(json!({"id":format!("{method}/{name}"),"request":request,"independent_expected":independent_expected,"status":if matched {"passed"} else {"failed"},"classification":if matched && oracle.as_ref().is_ok_and(|v| v.get("error_code").is_some()) {"paired-expected-error"} else if matched {"matched"} else {"mismatch"},"oracle":oracle.as_ref().ok(),"candidate":candidate.as_ref().ok(),"oracle_error":oracle.as_ref().err().map(ToString::to_string),"candidate_error":candidate.as_ref().err().map(ToString::to_string)}));
        if !matched {
            failures.push(format!(
                "{method}/{name}: oracle={oracle:?} candidate={candidate:?}"
            ));
        }
    }
    for format in [
        "PROFILE_FORMAT_FLAMEGRAPH",
        "PROFILE_FORMAT_TREE",
        "PROFILE_FORMAT_DOT",
        "PROFILE_FORMAT_PPROF",
    ] {
        let mut request = base.clone();
        request["format"] = json!(format);
        request["async"] = json!({"type":"ASYNC_QUERY_TYPE_FORCE"});
        let expected = profile_async_lifecycle(&client, &oracle_base, None, request.clone()).await;
        let actual = profile_async_lifecycle(
            &client,
            &candidate.querier_base,
            Some(TENANT),
            request.clone(),
        )
        .await;
        let result = match format {
            "PROFILE_FORMAT_TREE" => {
                json!({"tree":BASE64.encode(b"\x00\x00\x01\x09main.work\x28\x01\x0cmain.hotloop\x64\x00")})
            }
            "PROFILE_FORMAT_DOT" => profile_fixture_dot_expected(),
            "PROFILE_FORMAT_PPROF" => {
                json!({"pprof":[[[FUNC_WORK],40],[[FUNC_WORK,FUNC_HOT],100]]})
            }
            _ => {
                json!({"stacks":[[[FUNC_WORK],40],[[FUNC_WORK,FUNC_HOT],100]],"total":140,"max_self":100})
            }
        };
        let independent = json!({"submission_status":"ASYNC_QUERY_STATUS_IN_PROGRESS","completion_status":"ASYNC_QUERY_STATUS_SUCCESS","result":result});
        let matched = match (&expected, &actual) {
            (Ok((expected, _)), Ok((actual, _))) => {
                *expected == independent && *actual == independent
            }

            _ => false,
        };
        evidence["cases"].as_array_mut().ok_or("evidence cases missing")?.push(json!({"id":format!("SelectMergeStacktraces/async {format}"),"request":request,"independent_expected":independent,"status":if matched {"passed"} else {"failed"},"classification":if matched {"matched"} else {"mismatch"},"oracle":expected.as_ref().ok().map(|(value,_)|value),"candidate":actual.as_ref().ok().map(|(value,_)|value),"exchanges":{"oracle":expected.as_ref().ok().map(|(_,value)|value),"candidate":actual.as_ref().ok().map(|(_,value)|value)},"oracle_error":expected.as_ref().err().map(ToString::to_string),"candidate_error":actual.as_ref().err().map(ToString::to_string)}));
        if !matched {
            failures.push(format!(
                "async {format}: expected={expected:?} actual={actual:?}"
            ));
        }
    }
    evidence["status"] = json!(if failures.is_empty() {
        "passed"
    } else {
        "failed"
    });
    if let Some(dir) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        std::fs::write(
            std::path::PathBuf::from(dir).join("pyroscope-v2-fields.json"),
            serde_json::to_vec_pretty(&evidence)?,
        )?;
    }
    candidate.shutdown();
    drop(oracle);
    profile_architecture_controls(&client, time).await?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n").into())
    }
}

fn profile_v2_independent_witness(
    method: &str,
    name: &str,
    time: i64,
    oracle: &TestResult<Value>,
    candidate: &TestResult<Value>,
) -> (Value, bool) {
    let mut independent_expected = Value::Null;
    let independent = if method == "SelectMergeStacktraces"
        && oracle.as_ref().is_ok_and(|v| v.get("stacks").is_some())
        && name != "bounded nodes"
    {
        let expected = match name {
            "leaf is not root"
            | "reordered prefix"
            | "missing profile ID"
            | "missing modern span"
            | "start excludes profile"
            | "end excludes profile"
            | "missing profile type" => json!([]),
            "hot prefix" | "hot trace" | "hot modern span" => {
                json!([[[FUNC_WORK, FUNC_HOT], 100]])
            }
            _ => json!([[[FUNC_WORK], 40], [[FUNC_WORK, FUNC_HOT], 100]]),
        };
        independent_expected = json!({"stacks":expected});
        oracle.as_ref().is_ok_and(|v| v["stacks"] == expected)
            && candidate.as_ref().is_ok_and(|v| v["stacks"] == expected)
    } else if method == "SelectMergeStacktraces" && name == "PROFILE_FORMAT_TREE" {
        // The fixture's named tree has a virtual empty root, work self40
        // and hotloop self100. These are independent UTF8/varint wire bytes.
        let expected = json!({"tree":BASE64.encode(b"\x00\x00\x01\x09main.work\x28\x01\x0cmain.hotloop\x64\x00")});
        independent_expected = expected.clone();
        oracle.as_ref().is_ok_and(|value| *value == expected)
            && candidate.as_ref().is_ok_and(|value| *value == expected)
    } else if method == "SelectMergeStacktraces" && name == "PROFILE_FORMAT_DOT" {
        let expected = profile_fixture_dot_expected();
        independent_expected = expected.clone();
        oracle.as_ref().is_ok_and(|value| *value == expected)
            && candidate.as_ref().is_ok_and(|value| *value == expected)
    } else if method == "SelectMergeStacktraces" && name == "PROFILE_FORMAT_PPROF" {
        let expected = json!({"pprof":[[[FUNC_WORK],40],[[FUNC_WORK,FUNC_HOT],100]]});
        independent_expected = expected.clone();
        oracle.as_ref().is_ok_and(|value| *value == expected)
            && candidate.as_ref().is_ok_and(|value| *value == expected)
    } else if method == "SelectMergeProfile" {
        let empty = matches!(name, "empty" | "absent UUID");
        let leaf = matches!(name, "GoPGO leaf" | "GoPGO aggregate");
        let trace = name == "trace selector";
        let stacks = if empty {
            json!([])
        } else if leaf {
            json!([[[FUNC_HOT], 100], [[FUNC_WORK], 40]])
        } else if trace {
            json!([[[FUNC_WORK, FUNC_HOT], 100]])
        } else {
            json!([[[FUNC_WORK], 40], [[FUNC_WORK, FUNC_HOT], 100]])
        };
        let locations = if empty {
            json!([])
        } else if trace {
            json!([
            {"address":4096,"lines":[{"name":FUNC_WORK,"file":"app.go","line":10}]},
            {"address":8192,"lines":[{"name":FUNC_HOT,"file":"app.go","line":20}]}])
        } else {
            let aggregate = matches!(name, "GoPGO aggregate" | "GoPGO aggregate with caller");
            let mut locations = vec![
                json!({"address":0,"lines":[{"name":FUNC_HOT,"file":"app.go","line":if aggregate {0} else {20}}]}),
                json!({"address":0,"lines":[{"name":FUNC_WORK,"file":"app.go","line":if aggregate {0} else {10}}]}),
            ];
            if name == "bounded nodes" {
                locations.push(json!({"address":0,"lines":[{"name":"other","file":"","line":0}]}));
            }
            locations.sort_by_key(ToString::to_string);
            json!(locations)
        };
        let expected = json!({"stacks":stacks,"locations":locations});
        independent_expected = expected.clone();
        oracle.as_ref().is_ok_and(|value| *value == expected)
            && candidate.as_ref().is_ok_and(|value| *value == expected)
    } else if method == "SelectSeries"
        && matches!(name, "half second resolution" | "two second resolution")
    {
        let endpoint = if name == "half second resolution" {
            time
        } else {
            time + 1000
        };
        independent_expected =
            json!({"point_timestamp":endpoint,"point_value":140.0,"point_count":1});
        let witness = |value: &Value| {
            let points: Vec<_> = value["series"]
                .as_array()
                .into_iter()
                .flatten()
                .flat_map(|series| series["points"].as_array().into_iter().flatten())
                .collect();
            points.len() == 1
                && json_i64(&points[0]["timestamp"]) == Some(endpoint)
                && points[0]["value"] == 140.0
        };
        oracle.as_ref().is_ok_and(witness) && candidate.as_ref().is_ok_and(witness)
    } else if method == "SelectSeries" && matches!(name, "SUM" | "AVERAGE" | "grouping" | "limit") {
        let expected = match name {
            "SUM" | "AVERAGE" => json!([190.0]),
            "grouping" => json!([140.0, 50.0]),
            _ => json!([140.0]),
        };
        independent_expected = json!({"point_values":expected});
        let values = |response: &Value| {
            json!(
                response["series"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .flat_map(|series| series["points"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|point| point["value"].clone()))
                    .collect::<Vec<_>>()
            )
        };
        oracle.as_ref().is_ok_and(|value| values(value) == expected)
            && candidate
                .as_ref()
                .is_ok_and(|value| values(value) == expected)
    } else if method == "SelectSeries" && name == "empty span exemplars" {
        independent_expected = json!({"series_count":0});
        oracle.as_ref().is_ok_and(|value| *value == json!({}))
            && candidate.as_ref().is_ok_and(|value| *value == json!({}))
    } else if method == "SelectHeatmap" && matches!(name, "two groups" | "top group limit") {
        let expected = if name == "two groups" { 2 } else { 1 };
        independent_expected = json!({"series_count":expected,"profile_count":expected});
        let witness = |value: &Value| {
            value["series"].as_array().is_some_and(|series| {
                series.len() == expected
                    && series
                        .iter()
                        .flat_map(|series| series["slots"].as_array().into_iter().flatten())
                        .flat_map(|slot| slot["counts"].as_array().into_iter().flatten())
                        .filter_map(Value::as_u64)
                        .sum::<u64>()
                        == u64::try_from(expected).unwrap_or_default()
            })
        };
        oracle.as_ref().is_ok_and(witness) && candidate.as_ref().is_ok_and(witness)
    } else {
        true
    };
    (independent_expected, independent)
}

// DOT describes function self percentages and rounds edge widths upward.
// The independent fixture ledger has work40 + hotloop100, with work->hotloop.
fn profile_fixture_dot_expected() -> Value {
    let work = json!({"function":FUNC_WORK,"file":"app.go","line":10,"address":4096,"self_percent":format!("{:.2}",40.0/140.0*100.0)});
    let hot = json!({"function":FUNC_HOT,"file":"app.go","line":20,"address":8192,"self_percent":format!("{:.2}",100.0/140.0*100.0)});
    json!({"nodes":[hot,work],"edges":[{"caller":work,"callee":hot,"weight":72}]})
}

async fn profile_architecture_controls(client: &reqwest::Client, time: i64) -> TestResult {
    use prost::Message;
    for (architecture, options, artifact) in [
        (
            query::PyroscopeQueryArchitecture::V1,
            vec!["-architecture.storage=v1", "-write-path=ingester"],
            "pyroscope-v1-aggregation.json",
        ),
        (
            query::PyroscopeQueryArchitecture::V2,
            vec![
                "-architecture.storage=v2",
                "-write-path=segment-writer",
                "-query-frontend.async-queries-enabled=false",
            ],
            "pyroscope-v2-async-disabled.json",
        ),
    ] {
        let oracle = start_pyroscope_with_options(&options).await?;
        let oracle_base = mapped_base_url(&oracle, PYROSCOPE_HTTP_PORT).await?;
        wait_for_http_ok(client, &oracle_base, &["/ready"]).await?;
        let sink = CapturingSink::default();
        let store = WalTailProfileStore::new();
        let candidate = start_krabka_pair_with_query_options(
            sink.clone(),
            store.clone(),
            QuerierQueryMode {
                architecture,
                async_queries_enabled: false,
                query_analysis_series_enabled: false,
            },
        )
        .await?;
        for (service, values) in [
            ("architecture-primary", [100, 40]),
            ("architecture-secondary", [35, 15]),
        ] {
            let mut export = pb::otlp_profiles::ExportProfilesServiceRequest::decode(
                otlp_export_body(u64::try_from(time)? * 1_000_000, service, false)?.as_slice(),
            )?;
            let samples = &mut export.resource_profiles[0].scope_profiles[0].profiles[0].samples;
            samples[0].values = vec![values[0]];
            samples[1].values = vec![values[1]];
            for (base, tenant) in [
                (&oracle_base, None),
                (&candidate.distributor_base, Some(TENANT)),
            ] {
                post_otlp_export(
                    client,
                    base,
                    tenant,
                    &export.encode_to_vec(),
                    "application/x-protobuf",
                    None,
                )
                .await?;
            }
        }
        drain_sink_into_cold_store(&sink)?;
        let base = json!({"profileTypeID":OTLP_PROFILE_TYPE,"labelSelector":"{service_name=~\"architecture-.*\"}","start":time-1000,"end":time+1000});
        connect_json_until(
            client,
            &oracle_base,
            None,
            "SelectMergeStacktraces",
            base.clone(),
            |value| value["flamegraph"]["total"].as_str() == Some("190"),
        )
        .await?;
        let mut report = json!({"suite":artifact.trim_end_matches(".json"),"upstream_revision":"7aeaa0ff91e83538b3ff0d09bfefb168bddc022d","upstream":{"image_tag":std::env::var("KRABKA_PYROSCOPE_IMAGE_TAG").ok(),"image_id":std::env::var("KRABKA_PYROSCOPE_IMAGE_ID").ok()},"settings":{"query_architecture":if architecture==query::PyroscopeQueryArchitecture::V1 {"v1"} else {"v2"},"oracle_async_enabled":false,"candidate_async_enabled":false,"oracle_query_analysis_series_enabled":false,"candidate_query_analysis_series_enabled":false},"fixture":{"primary_total":140,"secondary_total":50},"planned":2,"status":"running","cases":[]});
        let mut failures = Vec::new();
        for (name, field, value, expected) in
            if architecture == query::PyroscopeQueryArchitecture::V1 {
                [
                    (
                        "SUM",
                        "aggregation",
                        "TIME_SERIES_AGGREGATION_TYPE_SUM",
                        190.0,
                    ),
                    (
                        "AVERAGE",
                        "aggregation",
                        "TIME_SERIES_AGGREGATION_TYPE_AVERAGE",
                        95.0,
                    ),
                ]
            } else {
                [
                    (
                        "FORCE falls through",
                        "async",
                        "ASYNC_QUERY_TYPE_FORCE",
                        190.0,
                    ),
                    (
                        "DISABLED falls through",
                        "async",
                        "ASYNC_QUERY_TYPE_DISABLED",
                        190.0,
                    ),
                ]
            }
        {
            let mut request = base.clone();
            let method = if field == "aggregation" {
                request["step"] = json!(1.0);
                request[field] = json!(value);
                "SelectSeries"
            } else {
                request[field] = json!({"type":value});
                "SelectMergeStacktraces"
            };
            let oracle =
                profile_v2_response(client, &oracle_base, None, method, request.clone()).await;
            let candidate = profile_v2_response(
                client,
                &candidate.querier_base,
                Some(TENANT),
                method,
                request.clone(),
            )
            .await;
            let witness = |response: &Value| {
                if method == "SelectSeries" {
                    response["series"][0]["points"][0]["value"].as_f64() == Some(expected)
                } else {
                    response["total"].as_i64() == Some(190) && response.get("async").is_none()
                }
            };
            let matched =
                matches!((&oracle,&candidate),(Ok(a),Ok(b)) if a==b&&witness(a)&&witness(b));
            report["cases"].as_array_mut().ok_or("cases missing")?.push(json!({"id":format!("{method}/{name}"),"request":request,"independent_expected":if method=="SelectSeries" {json!({"point_values":[expected]})} else {json!({"total":190,"async":null})},"classification":if matched {"matched"} else {"mismatch"},"status":if matched {"passed"} else {"failed"},"oracle":oracle.as_ref().ok(),"candidate":candidate.as_ref().ok(),"oracle_error":oracle.as_ref().err().map(ToString::to_string),"candidate_error":candidate.as_ref().err().map(ToString::to_string)}));
            if !matched {
                failures.push(format!("{artifact}/{name}: {oracle:?} vs {candidate:?}"));
            }
        }
        if architecture == query::PyroscopeQueryArchitecture::V1 {
            report["planned"] = json!(4);
            for (name, query) in [
                (
                    "analysis disabled matching",
                    format!("{OTLP_PROFILE_TYPE}{{service_name=~\"architecture-.*\"}}"),
                ),
                ("analysis disabled malformed", "invalid ignored {".into()),
            ] {
                let request = json!({"query":query,"start":time-1000,"end":time+1000});
                let oracle =
                    connect_json(client, &oracle_base, None, "AnalyzeQuery", request.clone()).await;
                let actual = connect_json(
                    client,
                    &candidate.querier_base,
                    Some(TENANT),
                    "AnalyzeQuery",
                    request.clone(),
                )
                .await;
                let witness = |response: &Value| {
                    response
                        .pointer("/queryImpact/totalQueriedSeries")
                        .and_then(json_i64)
                        .unwrap_or_default()
                        == 0
                        && response["queryScopes"]
                            .as_array()
                            .is_some_and(|scopes| scopes.len() == 2)
                };
                let matched =
                    oracle.as_ref().is_ok_and(witness) && actual.as_ref().is_ok_and(witness);
                let normalized = json!({"total_queried_series":0,"component_types":["Short term storage","Long term storage"]});
                report["cases"].as_array_mut().ok_or("cases missing")?.push(json!({"id":format!("AnalyzeQuery/{name}"),"request":request,"independent_expected":normalized,"comparison_fields":["query_impact.total_queried_series","query_scopes.component_type"],"classification":if matched {"matched"} else {"mismatch"},"status":if matched {"passed"} else {"failed"},"oracle":if oracle.as_ref().is_ok_and(witness) {Some(&normalized)} else {None},"candidate":if actual.as_ref().is_ok_and(witness) {Some(&normalized)} else {None},"exchanges":{"oracle":oracle.as_ref().ok(),"candidate":actual.as_ref().ok()},"oracle_error":oracle.as_ref().err().map(ToString::to_string),"candidate_error":actual.as_ref().err().map(ToString::to_string)}));
                if !matched {
                    failures.push(format!("{artifact}/{name}: {oracle:?} vs {actual:?}"));
                }
            }
        }
        report["status"] = json!(if failures.is_empty() {
            "passed"
        } else {
            "failed"
        });
        if let Some(dir) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
            std::fs::write(
                std::path::PathBuf::from(dir).join(artifact),
                serde_json::to_vec_pretty(&report)?,
            )?;
        }
        candidate.shutdown();
        if !failures.is_empty() {
            return Err(failures.join("\n").into());
        }
    }
    Ok(())
}

async fn profile_v2_fixtures(
    client: &reqwest::Client,
    oracle_base: &str,
    candidate: &KrabkaPair,
    sink: &CapturingSink,
    time: i64,
) -> TestResult<(String, Value)> {
    use pb::otlp_profiles::{ExportProfilesServiceRequest, Link};
    use prost::Message;
    let mut export = ExportProfilesServiceRequest::decode(
        otlp_export_body(u64::try_from(time)? * 1_000_000, "v2-matrix", false)?.as_slice(),
    )?;
    export
        .dictionary
        .as_mut()
        .ok_or("fixture dictionary missing")?
        .link_table = vec![
        Link::default(),
        Link {
            trace_id: vec![1; 16],
            span_id: 42_u64.to_be_bytes().to_vec(),
        },
        Link {
            trace_id: vec![2; 16],
            span_id: 43_u64.to_be_bytes().to_vec(),
        },
    ];
    let profile = &mut export.resource_profiles[0].scope_profiles[0].profiles[0];
    profile.profile_id = vec![3; 16];
    profile.samples[0].link_index = 1;
    profile.samples[1].link_index = 2;
    let mut second_export = export.clone();
    second_export.resource_profiles[0]
        .resource
        .as_mut()
        .ok_or("resource missing")?
        .attributes[0]
        .value
        .as_mut()
        .ok_or("service label missing")?
        .value = Some(
        pb::opentelemetry::proto::common::v1::any_value::Value::StringValue(
            "v2-matrix-other".into(),
        ),
    );
    second_export.resource_profiles[0].scope_profiles[0].profiles[0].samples[0].values = vec![35];
    second_export.resource_profiles[0].scope_profiles[0].profiles[0].samples[1].values = vec![15];
    for (base, tenant) in [
        (oracle_base, None),
        (&candidate.distributor_base, Some(TENANT)),
    ] {
        post_otlp_export(
            client,
            base,
            tenant,
            &export.encode_to_vec(),
            "application/x-protobuf",
            None,
        )
        .await?;
        post_otlp_export(
            client,
            base,
            tenant,
            &second_export.encode_to_vec(),
            "application/x-protobuf",
            None,
        )
        .await?;
    }
    post_cpu_profile_with_id(
        client,
        PushTarget::oracle(oracle_base),
        IdentifiedCpuProfile {
            gzipped_pprof: &synthetic_cpu_pprof(time * 1_000_000)?,
            profile_id: "03030303-0303-0303-0303-030303030303",
        },
    )
    .await?;
    let identity = connect_json_until(client, oracle_base, None, "SelectSeries", json!({"profileTypeID":CPU_PROFILE_TYPE,"labelSelector":E2E_SELECTOR,"start":time-1000,"end":time+1000,"step":1.0,"exemplarType":"EXEMPLAR_TYPE_INDIVIDUAL"}),
        |value| value["series"][0]["points"][0]["exemplars"][0]["profileId"].as_str().is_some()).await?;
    let profile_id = identity["series"][0]["points"][0]["exemplars"][0]["profileId"]
        .as_str()
        .ok_or("oracle assigned profile ID missing")?
        .to_string();
    assert_eq!(identity["series"][0]["points"][0]["value"], 140.0);
    post_cpu_profile_with_id(
        client,
        PushTarget::krabka(&candidate.distributor_base),
        IdentifiedCpuProfile {
            gzipped_pprof: &synthetic_cpu_pprof(time * 1_000_000)?,
            profile_id: &profile_id,
        },
    )
    .await?;
    drain_sink_into_cold_store(sink)?;
    Ok((profile_id, identity))
}

fn profile_v2_request_cases(
    base: &Value,
    time: i64,
    profile_id: &str,
    selector: &str,
) -> TestResult<Vec<(&'static str, String, Value)>> {
    let mut cases: Vec<(&str, String, Value)> = Vec::new();
    for (name, fields) in [
        ("default", json!({})),
        (
            "root prefix",
            json!({"stackTraceSelector":{"callSite":[{"name":FUNC_WORK}]}}),
        ),
        (
            "hot prefix",
            json!({"stackTraceSelector":{"callSite":[{"name":FUNC_WORK},{"name":FUNC_HOT}]}}),
        ),
        (
            "leaf is not root",
            json!({"stackTraceSelector":{"callSite":[{"name":FUNC_HOT}]}}),
        ),
        (
            "reordered prefix",
            json!({"stackTraceSelector":{"callSite":[{"name":FUNC_HOT},{"name":FUNC_WORK}]}}),
        ),
        ("profile ID", json!({"profileIdSelector":[profile_id]})),
        (
            "missing profile ID",
            json!({"profileIdSelector":["04040404-0404-0404-0404-040404040404"]}),
        ),
        (
            "hot trace",
            json!({"traceIdSelector":["01010101010101010101010101010101"]}),
        ),
        (
            "both traces",
            json!({"traceIdSelector":["01010101010101010101010101010101","02020202020202020202020202020202"]}),
        ),
        (
            "hot modern span",
            json!({"spanSelector":["000000000000002a"]}),
        ),
        (
            "missing modern span",
            json!({"spanSelector":["000000000000002c"]}),
        ),
        ("bounded nodes", json!({"maxNodes":2})),
        (
            "explicit disabled async",
            json!({"async":{"type":"ASYNC_QUERY_TYPE_DISABLED","requestId":"ignored"}}),
        ),
    ] {
        let mut request = base.clone();
        request
            .as_object_mut()
            .ok_or("request missing")?
            .extend(fields.as_object().ok_or("fields missing")?.clone());
        if name.contains("profile ID") {
            request["profileTypeID"] = json!(CPU_PROFILE_TYPE);
            request["labelSelector"] = json!(E2E_SELECTOR);
        }
        cases.push(("SelectMergeStacktraces", name.into(), request));
    }
    for format in [
        "PROFILE_FORMAT_UNSPECIFIED",
        "PROFILE_FORMAT_FLAMEGRAPH",
        "PROFILE_FORMAT_TREE",
        "PROFILE_FORMAT_DOT",
        "PROFILE_FORMAT_PPROF",
    ] {
        let mut request = base.clone();
        request["format"] = json!(format);
        cases.push(("SelectMergeStacktraces", format.into(), request));
    }
    for (name, fields) in [
        ("start boundary inclusive", json!({"start":time})),
        ("end boundary inclusive", json!({"end":time})),
        ("start excludes profile", json!({"start":time+1})),
        ("end excludes profile", json!({"end":time-1})),
        (
            "missing profile type",
            json!({"profileTypeID":"absent:cpu:nanoseconds:cpu:nanoseconds"}),
        ),
        (
            "incompatible trace and span",
            json!({"traceIdSelector":["01010101010101010101010101010101"],"spanSelector":["000000000000002a"]}),
        ),
    ] {
        let mut request = base.clone();
        request
            .as_object_mut()
            .ok_or("request missing")?
            .extend(fields.as_object().ok_or("fields missing")?.clone());
        cases.push(("SelectMergeStacktraces", name.into(), request));
    }
    append_profile_v2_timeseries_cases(&mut cases, base, time)?;
    append_profile_v2_merge_profile_cases(&mut cases, base, profile_id, selector)?;
    for (method, name, request) in [
        (
            "ProfileTypes",
            "populated",
            json!({"start":time-1000,"end":time+1000}),
        ),
        ("ProfileTypes", "unbounded", json!({})),
        (
            "ProfileTypes",
            "empty",
            json!({"start":time+3000,"end":time+4000}),
        ),
        (
            "LabelValues",
            "filtered",
            json!({"name":"service_name","matchers":[selector],"start":time-1000,"end":time+1000}),
        ),
        (
            "LabelValues",
            "absent label",
            json!({"name":"absent","matchers":[selector],"start":time-1000,"end":time+1000}),
        ),
        (
            "LabelValues",
            "empty",
            json!({"name":"service_name","matchers":["{service_name=\"missing\"}"],"start":time-1000,"end":time+1000}),
        ),
        (
            "LabelNames",
            "filtered",
            json!({"matchers":[selector],"start":time-1000,"end":time+1000}),
        ),
        (
            "LabelNames",
            "empty",
            json!({"matchers":["{service_name=\"missing\"}"],"start":time-1000,"end":time+1000}),
        ),
        (
            "Series",
            "projected",
            json!({"matchers":[selector],"labelNames":["service_name","__profile_type__"],"start":time-1000,"end":time+1000}),
        ),
        (
            "Series",
            "full",
            json!({"matchers":[E2E_SELECTOR],"start":time-1000,"end":time+1000}),
        ),
        (
            "Series",
            "empty",
            json!({"matchers":["{service_name=\"missing\"}"],"start":time-1000,"end":time+1000}),
        ),
        ("GetProfileStats", "populated", json!({})),
    ] {
        cases.push((method, name.into(), request));
    }
    append_profile_v2_span_diff_analysis_cases(&mut cases, base, time, profile_id, selector)?;
    let mut bounded_profile = base.clone();
    bounded_profile["profileTypeID"] = json!(CPU_PROFILE_TYPE);
    bounded_profile["labelSelector"] = json!(E2E_SELECTOR);
    bounded_profile["maxNodes"] = json!(1);
    cases.push((
        "SelectMergeProfile",
        "bounded nodes".into(),
        bounded_profile,
    ));
    cases.push((
        "SelectMergeStacktraces",
        "invalid async request ID".into(),
        json!({"async":{"type":"ASYNC_QUERY_TYPE_FORCE","requestId":"missing-query"}}),
    ));
    cases.push(("SelectMergeStacktraces", "unknown async request ID".into(), json!({"async":{"type":"ASYNC_QUERY_TYPE_FORCE","requestId":"04040404-0404-4404-8404-040404040404"}})));
    Ok(cases)
}

fn append_profile_v2_merge_profile_cases(
    cases: &mut Vec<(&'static str, String, Value)>,
    base: &Value,
    profile_id: &str,
    selector: &str,
) -> TestResult {
    for (name, fields) in [
        ("default", json!({})),
        ("profile UUID", json!({"profileIdSelector":[profile_id]})),
        (
            "absent UUID",
            json!({"profileIdSelector":["04040404-0404-0404-0404-040404040404"]}),
        ),
        (
            "GoPGO leaf",
            json!({"stackTraceSelector":{"goPgo":{"keepLocations":1}}}),
        ),
        (
            "GoPGO aggregate",
            json!({"stackTraceSelector":{"goPgo":{"keepLocations":1,"aggregateCallees":true}}}),
        ),
        (
            "GoPGO aggregate with caller",
            json!({"stackTraceSelector":{"goPgo":{"keepLocations":2,"aggregateCallees":true}}}),
        ),
        (
            "GoPGO default",
            json!({"stackTraceSelector":{"callSite":[{"name":"does-not-exist"}],"goPgo":{"keepLocations":0}}}),
        ),
        (
            "trace selector",
            json!({"traceIdSelector":["01010101010101010101010101010101"]}),
        ),
        (
            "empty",
            json!({"labelSelector":"{service_name=\"missing\"}"}),
        ),
    ] {
        let mut request = base.clone();
        request["profileTypeID"] = json!(CPU_PROFILE_TYPE);
        request["labelSelector"] = json!(E2E_SELECTOR);
        request
            .as_object_mut()
            .ok_or("request missing")?
            .extend(fields.as_object().ok_or("fields missing")?.clone());
        if name == "trace selector" {
            request["profileTypeID"] = json!(OTLP_PROFILE_TYPE);
            request["labelSelector"] = json!(selector);
        }
        cases.push(("SelectMergeProfile", name.into(), request));
    }
    Ok(())
}

fn append_profile_v2_timeseries_cases(
    cases: &mut Vec<(&'static str, String, Value)>,
    base: &Value,
    time: i64,
) -> TestResult {
    for (name, fields) in [
        (
            "SUM",
            json!({"aggregation":"TIME_SERIES_AGGREGATION_TYPE_SUM"}),
        ),
        (
            "AVERAGE",
            json!({"aggregation":"TIME_SERIES_AGGREGATION_TYPE_AVERAGE"}),
        ),
        ("grouping", json!({"groupBy":["service_name"]})),
        (
            "individual exemplars",
            json!({"exemplarType":"EXEMPLAR_TYPE_INDIVIDUAL"}),
        ),
        (
            "span exemplars",
            json!({"exemplarType":"EXEMPLAR_TYPE_SPAN"}),
        ),
        (
            "stack filter",
            json!({"stackTraceSelector":{"callSite":[{"name":FUNC_WORK},{"name":FUNC_HOT}]}}),
        ),
        ("limit", json!({"limit":1})),
        (
            "empty span exemplars",
            json!({"labelSelector":"{service_name=\"missing\"}","exemplarType":"EXEMPLAR_TYPE_SPAN"}),
        ),
    ] {
        let mut request = base.clone();
        request["step"] = json!(1.0);
        request
            .as_object_mut()
            .ok_or("request missing")?
            .extend(fields.as_object().ok_or("fields missing")?.clone());
        if matches!(name, "SUM" | "AVERAGE" | "grouping" | "limit") {
            request["labelSelector"] = json!("{service_name=~\"v2-matrix.*\"}");
        }
        if name == "limit" {
            request["groupBy"] = json!(["service_name"]);
        }
        if name == "individual exemplars" {
            request["profileTypeID"] = json!(CPU_PROFILE_TYPE);
            request["labelSelector"] = json!(E2E_SELECTOR);
        }
        cases.push(("SelectSeries", name.into(), request));
    }
    for (name, step) in [
        ("half second resolution", 0.5),
        ("two second resolution", 2.0),
    ] {
        let mut request = base.clone();
        request["step"] = json!(step);
        cases.push(("SelectSeries", name.into(), request));
    }
    for query_type in [
        "HEATMAP_QUERY_TYPE_UNSPECIFIED",
        "HEATMAP_QUERY_TYPE_INDIVIDUAL",
        "HEATMAP_QUERY_TYPE_SPAN",
    ] {
        for exemplar_type in [
            "EXEMPLAR_TYPE_UNSPECIFIED",
            "EXEMPLAR_TYPE_NONE",
            "EXEMPLAR_TYPE_INDIVIDUAL",
            "EXEMPLAR_TYPE_SPAN",
        ] {
            let mut request = base.clone();
            request["step"] = json!(1.0);
            request["queryType"] = json!(query_type);
            request["exemplarType"] = json!(exemplar_type);
            request["groupBy"] = json!(["service_name"]);
            request["limit"] = json!(1);
            if query_type == "HEATMAP_QUERY_TYPE_INDIVIDUAL" {
                request["profileTypeID"] = json!(CPU_PROFILE_TYPE);
                request["labelSelector"] = json!(E2E_SELECTOR);
            }
            cases.push((
                "SelectHeatmap",
                format!("{query_type}/{exemplar_type}"),
                request,
            ));
        }
    }
    for (name, limit) in [("two groups", 2), ("top group limit", 1)] {
        let mut request = base.clone();
        request["labelSelector"] = json!("{service_name=~\"v2-matrix.*\"}");
        request["queryType"] = json!("HEATMAP_QUERY_TYPE_INDIVIDUAL");
        request["exemplarType"] = json!("EXEMPLAR_TYPE_NONE");
        request["groupBy"] = json!(["service_name"]);
        request["step"] = json!(1.0);
        request["limit"] = json!(limit);
        cases.push(("SelectHeatmap", name.into(), request));
    }
    for (name, start, end) in [
        ("initial lookback", time + 123, time + 1500),
        ("exact end", time - 123, time),
        ("empty", time + 3000, time + 4000),
    ] {
        let mut request = base.clone();
        request["start"] = json!(start);
        request["end"] = json!(end);
        request["step"] = json!(1.0);
        request["queryType"] = json!("HEATMAP_QUERY_TYPE_SPAN");
        cases.push(("SelectHeatmap", name.into(), request));
    }
    Ok(())
}

fn append_profile_v2_span_diff_analysis_cases(
    cases: &mut Vec<(&'static str, String, Value)>,
    base: &Value,
    time: i64,
    profile_id: &str,
    selector: &str,
) -> TestResult {
    for (name, fields) in [
        (
            "hot deprecated span",
            json!({"spanSelector":["000000000000002a"]}),
        ),
        (
            "missing deprecated span",
            json!({"spanSelector":["000000000000002c"]}),
        ),
        (
            "bounded deprecated span",
            json!({"spanSelector":["000000000000002a"],"maxNodes":1}),
        ),
    ] {
        let mut request = base.clone();
        request
            .as_object_mut()
            .ok_or("request missing")?
            .extend(fields.as_object().ok_or("fields missing")?.clone());
        cases.push(("SelectMergeSpanProfile", name.into(), request));
    }
    for format in [
        "PROFILE_FORMAT_UNSPECIFIED",
        "PROFILE_FORMAT_FLAMEGRAPH",
        "PROFILE_FORMAT_TREE",
        "PROFILE_FORMAT_DOT",
        "PROFILE_FORMAT_PPROF",
    ] {
        let mut request = base.clone();
        request["spanSelector"] = json!(["000000000000002a"]);
        request["format"] = json!(format);
        cases.push(("SelectMergeSpanProfile", format.into(), request));
    }
    for name in [
        "empty left",
        "empty right",
        "both empty",
        "hot prefix",
        "linked traces",
        "linked spans",
        "profile IDs",
        "ignored formats and async",
        "bounded nodes",
    ] {
        let mut left = base.clone();
        let mut right = base.clone();
        match name {
            "empty left" => left["labelSelector"] = json!("{service_name=\"missing\"}"),
            "empty right" => right["labelSelector"] = json!("{service_name=\"missing\"}"),
            "both empty" => {
                left["labelSelector"] = json!("{service_name=\"missing\"}");
                right = left.clone();
            }
            "hot prefix" => {
                left["stackTraceSelector"] =
                    json!({"callSite":[{"name":FUNC_WORK},{"name":FUNC_HOT}]});
            }
            "linked traces" => {
                left["traceIdSelector"] = json!(["01010101010101010101010101010101"]);
                right["traceIdSelector"] = json!(["02020202020202020202020202020202"]);
            }
            "linked spans" => {
                left["spanSelector"] = json!(["000000000000002a"]);
                right["spanSelector"] = json!(["000000000000002b"]);
            }
            "profile IDs" => {
                left["profileTypeID"] = json!(CPU_PROFILE_TYPE);
                left["labelSelector"] = json!(E2E_SELECTOR);
                left["profileIdSelector"] = json!([profile_id]);
                right = left.clone();
                right["profileIdSelector"] = json!(["04040404-0404-0404-0404-040404040404"]);
            }
            "ignored formats and async" => {
                left["format"] = json!("PROFILE_FORMAT_TREE");
                right["format"] = json!("PROFILE_FORMAT_DOT");
                left["async"] = json!({"type":"ASYNC_QUERY_TYPE_FORCE","requestId":"ignored"});
            }
            "bounded nodes" => {
                left["maxNodes"] = json!(1);
                right["maxNodes"] = json!(2);
            }
            _ => unreachable!(),
        }
        cases.push(("Diff", name.into(), json!({"left":left,"right":right})));
    }
    for (name, request) in [
        (
            "matching query",
            json!({"query":format!("{OTLP_PROFILE_TYPE}{selector}"),"start":time-1000,"end":time+1000}),
        ),
        (
            "excluded time",
            json!({"query":format!("{OTLP_PROFILE_TYPE}{selector}"),"start":time+3000,"end":time+4000}),
        ),
        (
            "malformed ignored query",
            json!({"query":"invalid","start":10,"end":0}),
        ),
        ("omitted fields", json!({})),
    ] {
        cases.push(("AnalyzeQuery", name.into(), request));
    }
    Ok(())
}

async fn profile_v2_response(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    method: &str,
    body: Value,
) -> TestResult<Value> {
    let mut request = client
        .post(format!("{base}/querier.v1.QuerierService/{method}"))
        .header("content-type", "application/json")
        .header("connect-protocol-version", "1")
        .json(&body);
    if let Some(tenant) = tenant {
        request = request.header("x-scope-orgid", tenant);
    }
    let response = request.send().await?;
    let status = response.status();
    let response: Value = response.json().await?;
    if status != StatusCode::OK {
        return Ok(json!({"error_code": response["code"], "status": status.as_u16()}));
    }
    profile_v2_response_value(method, response)
}

fn profile_v2_response_value(method: &str, response: Value) -> TestResult<Value> {
    match method {
        "SelectMergeStacktraces" => {
            let response: pb::querier::v1::SelectMergeStacktracesResponse =
                serde_json::from_value(response)?;
            if let Some(flamegraph) = response.flamegraph {
                return Ok(
                    json!({"stacks":normalized_flamegraph_stacks(&flamegraph)?,"total":flamegraph.total,"max_self":flamegraph.max_self}),
                );
            }
            if let Some(pprof) = response.pprof {
                return Ok(
                    json!({"pprof":normalized_pprof_stacks(&pprof.profile.ok_or("pprof missing")?)?}),
                );
            }
            if !response.dot.is_empty() {
                return normalized_dot_graph(&response.dot);
            }
            Ok(serde_json::to_value(response)?)
        }
        "Diff" => {
            let response: pb::querier::v1::DiffResponse = serde_json::from_value(response)?;
            let graph = response.flamegraph.ok_or("Diff flamegraph missing")?;
            let mut value = serde_json::to_value(&graph)?;
            value["total"] = json!(graph.total);
            value["maxSelf"] = json!(graph.max_self);
            value["leftTicks"] = json!(graph.left_ticks);
            value["rightTicks"] = json!(graph.right_ticks);
            canonical_diff(&json!({"flamegraph":value}))
        }
        "AnalyzeQuery" => {
            let response: pb::querier::v1::AnalyzeQueryResponse = serde_json::from_value(response)?;
            Ok(serde_json::to_value(response)?)
        }
        "SelectMergeSpanProfile" => {
            let response: pb::querier::v1::SelectMergeSpanProfileResponse =
                serde_json::from_value(response)?;
            if let Some(flamegraph) = response.flamegraph {
                return Ok(
                    json!({"stacks":normalized_flamegraph_stacks(&flamegraph)?,"total":flamegraph.total,"max_self":flamegraph.max_self}),
                );
            }
            Ok(serde_json::to_value(response)?)
        }
        "ProfileTypes" => {
            let mut response: pb::querier::v1::ProfileTypesResponse =
                serde_json::from_value(response)?;
            response.profile_types.sort_by(|a, b| a.id.cmp(&b.id));
            Ok(serde_json::to_value(response)?)
        }
        "LabelNames" | "LabelValues" => {
            let mut response: pb::types::v1::LabelNamesResponse = serde_json::from_value(response)?;
            response.names.sort();
            Ok(serde_json::to_value(response)?)
        }
        "Series" => {
            let mut response: pb::querier::v1::SeriesResponse = serde_json::from_value(response)?;
            for set in &mut response.labels_set {
                set.labels
                    .sort_by(|a, b| (&a.name, &a.value).cmp(&(&b.name, &b.value)));
            }
            response
                .labels_set
                .sort_by_key(|set| serde_json::to_string(&set.labels).unwrap_or_default());
            Ok(serde_json::to_value(response)?)
        }
        "GetProfileStats" => {
            let response: pb::types::v1::GetProfileStatsResponse =
                serde_json::from_value(response)?;
            Ok(serde_json::to_value(response)?)
        }
        "SelectMergeProfile" => {
            let profile: pb::google::v1::Profile = serde_json::from_value(response)?;
            let stacks = normalized_pprof_stacks(&profile)?;
            let mut locations = Vec::new();
            for location in &profile.location {
                let mut lines = Vec::new();
                for line in &location.line {
                    let function = profile
                        .function
                        .iter()
                        .find(|function| function.id == line.function_id)
                        .ok_or("missing line function")?;
                    let name = profile
                        .string_table
                        .get(usize::try_from(function.name)?)
                        .ok_or("missing function name")?;
                    let file = profile
                        .string_table
                        .get(usize::try_from(function.filename)?)
                        .ok_or("missing function filename")?;
                    lines.push(json!({"name":name,"file":file,"line":line.line}));
                }
                locations.push(json!({"address":location.address,"lines":lines}));
            }
            locations.sort_by_key(ToString::to_string);
            Ok(json!({"stacks":stacks,"locations":locations}))
        }
        "SelectSeries" => {
            let mut response: pb::querier::v1::SelectSeriesResponse =
                serde_json::from_value(response)?;
            for series in &mut response.series {
                series
                    .labels
                    .sort_by(|a, b| (&a.name, &a.value).cmp(&(&b.name, &b.value)));
                for point in &mut series.points {
                    sort_exemplars(&mut point.exemplars);
                }
            }
            response
                .series
                .sort_by_key(|series| serde_json::to_string(&series.labels).unwrap_or_default());
            Ok(serde_json::to_value(response)?)
        }
        "SelectHeatmap" => {
            let mut response: pb::querier::v1::SelectHeatmapResponse =
                serde_json::from_value(response)?;
            for series in &mut response.series {
                series
                    .labels
                    .sort_by(|a, b| (&a.name, &a.value).cmp(&(&b.name, &b.value)));
                for slot in &mut series.slots {
                    sort_exemplars(&mut slot.exemplars);
                }
            }
            response
                .series
                .sort_by_key(|series| serde_json::to_string(&series.labels).unwrap_or_default());
            Ok(serde_json::to_value(response)?)
        }
        _ => Err(format!("unknown matrix RPC {method}").into()),
    }
}

fn normalized_dot_graph(dot: &str) -> TestResult<Value> {
    use std::collections::BTreeMap;
    let node = regex::Regex::new(
        r#"^(N[0-9]+) \[label=".*tooltip="([0-9a-f]+) ([^ ]+) ([^ ]+):([0-9]+) "#,
    )?;
    let percentage = regex::Regex::new(r"\(([0-9.]+)%\)")?;
    let edge = regex::Regex::new(r"^(N[0-9]+) -> (N[0-9]+).*weight=([0-9]+)")?;
    let mut nodes = BTreeMap::new();
    let mut edges = Vec::new();
    for line in dot.lines().map(str::trim) {
        if let Some(capture) = node.captures(line) {
            let percent = percentage
                .captures(line)
                .ok_or("DOT node lacks sample percentage")?[1]
                .to_string();
            let address = u64::from_str_radix(&capture[2], 16)?;
            nodes.insert(capture[1].to_string(),json!({"function":&capture[3],"file":&capture[4],"line":capture[5].parse::<i64>()?,"address":address,"self_percent":percent}));
        } else if let Some(capture) = edge.captures(line) {
            edges.push((
                capture[1].to_string(),
                capture[2].to_string(),
                capture[3].parse::<i64>()?,
            ));
        }
    }
    if nodes.is_empty() {
        return Err("DOT graph has no associated function nodes".into());
    }
    let mut functions = nodes.values().cloned().collect::<Vec<_>>();
    functions.sort_by_key(ToString::to_string);
    let mut calls=edges.into_iter().map(|(caller,callee,weight)|Ok(json!({"caller":nodes.get(&caller).ok_or("DOT caller missing")?,"callee":nodes.get(&callee).ok_or("DOT callee missing")?,"weight":weight}))).collect::<TestResult<Vec<_>>>()?;
    calls.sort_by_key(ToString::to_string);
    Ok(json!({"nodes":functions,"edges":calls}))
}

/// Server-generated IDs remain exact in the exchange witness. Equality compares
/// the lifecycle after checking each poll refers to its own submitted query.
async fn profile_async_lifecycle(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    request: Value,
) -> TestResult<(Value, Value)> {
    let submitted = connect_json(
        client,
        base,
        tenant,
        "SelectMergeStacktraces",
        request.clone(),
    )
    .await?;
    if submitted["async"]["status"] != "ASYNC_QUERY_STATUS_IN_PROGRESS"
        || submitted.get("flamegraph").is_some()
        || submitted.get("pprof").is_some()
        || submitted.get("dot").is_some()
        || submitted.get("tree").is_some()
    {
        return Err(format!("invalid async submission: {submitted}").into());
    }
    let id = submitted["async"]["requestId"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or("async request ID missing")?;
    let poll_request = json!({"profileTypeID":"ignored on poll","labelSelector":"invalid on poll","start":10,"end":0,"async":{"type":"ASYNC_QUERY_TYPE_FORCE","requestId":id}});
    let completed = connect_json_until(
        client,
        base,
        tenant,
        "SelectMergeStacktraces",
        poll_request.clone(),
        |response| response["async"]["status"] == "ASYNC_QUERY_STATUS_SUCCESS",
    )
    .await?;
    let result = profile_async_completion_value(&completed, id)?;
    Ok((
        json!({"submission_status":"ASYNC_QUERY_STATUS_IN_PROGRESS","completion_status":"ASYNC_QUERY_STATUS_SUCCESS","result":result}),
        json!({"submission":{"request":request,"response":submitted},"poll":{"request":poll_request,"response":completed}}),
    ))
}

fn profile_async_completion_value(completed: &Value, submitted_id: &str) -> TestResult<Value> {
    if completed["async"]["requestId"] != submitted_id
        || completed["async"]["status"] != "ASYNC_QUERY_STATUS_SUCCESS"
        || completed["async"].get("errorMessage").is_some()
    {
        return Err("async completion metadata changed".into());
    }
    // Normalize the successful exchange that is retained in the evidence.
    // A later polling request is a separate observation and may fail transiently.
    let mut result = profile_v2_response_value("SelectMergeStacktraces", completed.clone())?;
    if let Some(result) = result.as_object_mut() {
        result.remove("async");
    }
    Ok(result)
}

#[tokio::test]
async fn async_lifecycle_compares_recorded_success_and_rejects_result_drift() -> TestResult {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let id = "12345678-1234-1234-1234-123456789abc";
    let completed = json!({"async":{"requestId":id,"status":"ASYNC_QUERY_STATUS_SUCCESS"},"flamegraph":{"names":["total",FUNC_WORK,FUNC_HOT],"levels":[{"values":["0","140","0","0"]},{"values":["0","140","40","1"]},{"values":["40","100","100","2"]}],"total":"140","maxSelf":"100"}});
    let expected =
        json!({"stacks":[[[FUNC_WORK],40],[[FUNC_WORK,FUNC_HOT],100]],"total":140,"max_self":100});
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let captured = completed.clone();
    let router = axum::Router::new().route("/querier.v1.QuerierService/SelectMergeStacktraces", axum::routing::post(move |axum::Json(request):axum::Json<Value>| {
        let calls = Arc::clone(&observed_calls);
        let captured = captured.clone();
        async move {
            match calls.fetch_add(1,Ordering::SeqCst) {
                0 => (StatusCode::OK,axum::Json(json!({"async":{"requestId":id,"status":"ASYNC_QUERY_STATUS_IN_PROGRESS"}}))),
                1 => {
                    assert2::assert!(request["async"]["requestId"]==id);
                    (StatusCode::OK,axum::Json(captured))
                },
                _ => (StatusCode::INTERNAL_SERVER_ERROR,axum::Json(json!({"code":"internal","message":"a later poll failed"}))),
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let (actual, exchange) = profile_async_lifecycle(
        &reqwest::Client::new(),
        &base,
        None,
        json!({"async":{"type":"ASYNC_QUERY_TYPE_FORCE"}}),
    )
    .await?;
    assert2::assert!(calls.load(Ordering::SeqCst) == 2);
    assert2::assert!(actual["result"] == expected);
    assert2::assert!(exchange["poll"]["response"] == completed);
    // The provider's next request fails; it cannot replace the recorded success.
    let later = profile_v2_response(
        &reqwest::Client::new(),
        &base,
        None,
        "SelectMergeStacktraces",
        json!({"async":{"type":"ASYNC_QUERY_TYPE_FORCE","requestId":id}}),
    )
    .await?;
    assert2::assert!(later == json!({"error_code":"internal","status":500}));
    assert2::assert!(actual["result"] == expected);
    assert2::assert!(profile_async_completion_value(&completed, "different-id").is_err());
    let mut incomplete = completed.clone();
    incomplete["async"]["status"] = json!("ASYNC_QUERY_STATUS_IN_PROGRESS");
    assert2::assert!(profile_async_completion_value(&incomplete, id).is_err());
    incomplete = completed.clone();
    incomplete["async"]["errorMessage"] = json!("background failure");
    assert2::assert!(profile_async_completion_value(&incomplete, id).is_err());
    let mut changed = completed;
    changed["flamegraph"]["levels"][0]["values"][1] = json!("139");
    changed["flamegraph"]["levels"][1]["values"][1] = json!("139");
    changed["flamegraph"]["levels"][2]["values"][1] = json!("99");
    changed["flamegraph"]["levels"][2]["values"][2] = json!("99");
    changed["flamegraph"]["total"] = json!("139");
    changed["flamegraph"]["maxSelf"] = json!("99");
    assert2::assert!(profile_async_completion_value(&changed, id)? != expected);
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}
