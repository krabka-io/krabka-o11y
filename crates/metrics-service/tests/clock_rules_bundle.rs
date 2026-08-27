//! Drives the shipped KFC-8 clock rule bundle through the ruler it ships for.
//!
//! Every assertion here comes from an evaluation. The suite loads the bundle
//! from its data path, installs it through the ruler config API, seeds clock
//! readings, and reads back what the ruler records and what the alert API
//! renders. No assertion reads the text of the rule file.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use assert2::check;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use crabka_blockstore::Labels;
use crabka_metrics::{
    SamplePayload, WalRecord,
    distributor::clock_series,
    wire::{
        ClockSourceKind, ClockSyncState, DecodedClockReading, GnssFix, GnssReading, PtpReading,
        UnixNanos,
    },
};
use crabka_metrics_service::{
    evaluate_ruler_once, install_bundled_rule_groups, prometheus_api_state_for_store,
};
use crabka_promql::{
    AlertmanagerAlert, AlertmanagerSink, InMemoryMetricStore, PrometheusApiState,
    RecordingRuleWalSink, RulerAlertState, RulerAlertStateRecord, RulerGroupState,
    RulerGroupStateRecord, RulerShard, RulerStateSink, RulerWalError, prometheus_router,
};
use crabka_units::prelude::*;
use tower::ServiceExt as _;

const TENANT: &str = "tenant-a";

/// One five-hundred-and-twelfth of a second, in nanoseconds.
///
/// Every clock magnitude here is a whole multiple of this step. The step is a
/// power of two in seconds, so a sum or a difference of these magnitudes is
/// exact in binary floating point and an assertion needs no tolerance.
const STEP_NANOS: i64 = 1_953_125;

/// The first reading in the seeded window.
const START_MS: i64 = 1_700_000_000_000;

/// How long the seeded window runs. Every alert in the bundle holds for less
/// than half of this, so an evaluation at the middle and one at the end drive
/// the whole pending-to-firing path with samples in range at both times.
const WINDOW_MS: i64 = 30 * 60_000;

/// The interval between two seeded readings.
const READING_INTERVAL_MS: i64 = 60_000;

const MIDPOINT_MS: i64 = START_MS + WINDOW_MS / 2;
const END_MS: i64 = START_MS + WINDOW_MS;

/// One clock to seed, as the agent on a host would report it.
#[derive(Clone, Copy)]
struct ClockFixture {
    node: &'static str,
    source: ClockSourceKind,
    offset_steps: i64,
    uncertainty_steps: i64,
    sync_state: ClockSyncState,
    gnss_fix: Option<GnssFix>,
    step_total_steps: Option<i64>,
    clock_class: Option<u32>,
    /// The last moment this clock reports. A clock with no stop reports for the
    /// whole window.
    stops_after_ms: Option<i64>,
}

impl ClockFixture {
    const fn healthy(node: &'static str, offset_steps: i64, uncertainty_steps: i64) -> Self {
        Self {
            node,
            source: ClockSourceKind::Ptp,
            offset_steps,
            uncertainty_steps,
            sync_state: ClockSyncState::Synchronized,
            gnss_fix: None,
            step_total_steps: None,
            clock_class: None,
            stops_after_ms: None,
        }
    }

    const fn with_sync_state(mut self, sync_state: ClockSyncState) -> Self {
        self.sync_state = sync_state;
        self
    }

    const fn with_gnss_fix(mut self, fix: GnssFix) -> Self {
        self.source = ClockSourceKind::Gnss;
        self.gnss_fix = Some(fix);
        self
    }

    /// Reports a growing cumulative step magnitude, one step per reading.
    const fn with_clock_steps(mut self, steps: i64) -> Self {
        self.step_total_steps = Some(steps);
        self
    }

    /// Advertises a grandmaster class that changes on every reading.
    const fn with_flapping_clock_class(mut self, class: u32) -> Self {
        self.source = ClockSourceKind::Ptp;
        self.clock_class = Some(class);
        self
    }

    const fn stopping_after(mut self, timestamp_ms: i64) -> Self {
        self.stops_after_ms = Some(timestamp_ms);
        self
    }

    const fn reports_at(self, timestamp_ms: i64) -> bool {
        match self.stops_after_ms {
            Some(stop_ms) => timestamp_ms <= stop_ms,
            None => true,
        }
    }

    /// The seconds this fixture reports for an offset or an uncertainty.
    ///
    /// The projection converts nanoseconds to seconds, so the expected value of
    /// a recording rule takes the same conversion and the same arithmetic.
    fn seconds(steps: i64) -> f64 {
        Time::from_nanos(steps * STEP_NANOS).secs_f64()
    }

    fn offset_seconds(self) -> f64 {
        Self::seconds(self.offset_steps)
    }

    fn uncertainty_seconds(self) -> f64 {
        Self::seconds(self.uncertainty_steps)
    }

    fn reading(self, timestamp_ms: i64, tick: i64) -> DecodedClockReading {
        DecodedClockReading {
            node: self.node.to_string(),
            clock: "CLOCK_REALTIME".to_string(),
            source_kind: self.source,
            reading_unix_nanos: UnixNanos::from(timestamp_ms * 1_000_000),
            uncertainty_nanos: self.uncertainty_steps * STEP_NANOS,
            offset_nanos: self.offset_steps * STEP_NANOS,
            sync_state: self.sync_state,
            reference_id: None,
            last_sync_unix_nanos: None,
            frequency_ppb: None,
            // A cumulative step magnitude that grows once per reading. A
            // fixture with no step total reports the same value every time, so
            // `increase()` over it is zero.
            last_step_nanos: self
                .step_total_steps
                .map(|steps| steps * STEP_NANOS * tick.max(1)),
            ntp: None,
            ptp: self.clock_class.map(|class| PtpReading {
                mean_path_delay_nanos: STEP_NANOS,
                steps_removed: 1,
                // The advertised class alternates every reading, so
                // `changes()` over the window counts one change per reading.
                gm_clock_class: if tick % 2 == 0 { class } else { class + 1 },
                gm_clock_accuracy: 0x21,
            }),
            timex: None,
            gnss: self.gnss_fix.map(|fix| GnssReading {
                satellites_used: 9,
                fix: Some(fix),
            }),
        }
    }
}

/// The clock readings of a fleet that keeps every promise it makes.
///
/// The widest uncertainty is three steps, the widest interval runs from minus
/// three steps to plus five steps, and the brokers declare six steps.
fn healthy_fleet() -> Vec<ClockFixture> {
    vec![
        ClockFixture::healthy("node-a", 4, 1),
        ClockFixture::healthy("node-b", -2, 1),
        ClockFixture::healthy("node-c", 0, 3),
    ]
}

/// The uncertainty bound the brokers declare, in fixture steps.
///
/// Two brokers report, and the larger value wins.
const DECLARED_BOUND_STEPS: [i64; 2] = [6, 3];

fn labels(name: &str, pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    for (key, value) in pairs {
        labels.insert(*key, *value);
    }
    labels
}

/// Seeds one store with the projection of every reading in the window.
///
/// The seeding calls the same projection the ingest path calls, so the series
/// names and the labels the bundle selects are the ones the broker publishes.
fn store_with_fleet(fleet: &[ClockFixture], declared_bound_steps: &[i64]) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut tick = 0;
    let mut timestamp_ms = START_MS;
    while timestamp_ms <= END_MS {
        let readings = fleet
            .iter()
            .filter(|fixture| fixture.reports_at(timestamp_ms))
            .map(|fixture| fixture.reading(timestamp_ms, tick))
            .collect::<Vec<_>>();
        for series in clock_series(&readings, UnixNanos::from(timestamp_ms * 1_000_000)) {
            for sample in series.samples {
                store.push_float(
                    TENANT,
                    series.labels.clone(),
                    sample.timestamp_ms,
                    sample.value,
                );
            }
        }
        for (index, steps) in declared_bound_steps.iter().enumerate() {
            store.push_float(
                TENANT,
                labels(
                    "crabka_broker_delivery_clock_uncertainty_seconds",
                    &[("broker", &index.to_string())],
                ),
                timestamp_ms,
                ClockFixture::seconds(*steps),
            );
        }
        tick += 1;
        timestamp_ms += READING_INTERVAL_MS;
    }
    store
}

/// A ruler and the HTTP API in front of it, with the bundle installed.
struct Ruler {
    state: Arc<PrometheusApiState<InMemoryMetricStore>>,
    router: Router,
    wal: CapturedRecordings,
    alerts: CapturedAlerts,
    alert_state: RulerAlertState,
    group_state: RulerGroupState,
}

impl Ruler {
    async fn with_store(store: InMemoryMetricStore) -> Self {
        let state = prometheus_api_state_for_store(store);
        let router = prometheus_router(Arc::clone(&state));
        let installed = install_bundled_rule_groups(&router, &bundle_path(), TENANT)
            .await
            .expect("the shipped bundle installs");
        check!(
            installed
                == vec![
                    "krabka-clock".to_string(),
                    "krabka-clock-alerts".to_string()
                ]
        );
        Self {
            state,
            router,
            wal: CapturedRecordings::default(),
            alerts: CapturedAlerts::default(),
            alert_state: RulerAlertState::default(),
            group_state: RulerGroupState::default(),
        }
    }

    async fn with_fleet(fleet: &[ClockFixture]) -> Self {
        Self::with_store(store_with_fleet(fleet, &DECLARED_BOUND_STEPS)).await
    }

    /// Builds a ruler whose store already holds the recorded series, as a ruler
    /// that has run for a few cycles has.
    async fn with_derived_fleet(fleet: &[ClockFixture]) -> Self {
        Self::with_store(store_with_derived_series(fleet).await).await
    }

    /// Runs one ruler evaluation and returns the samples it recorded.
    async fn evaluate(&mut self, eval_time_ms: i64) -> Vec<WalRecord> {
        self.wal.take();
        evaluate_ruler_once(
            &self.state,
            (&self.wal, &self.alerts, &NoRulerState),
            &mut self.alert_state,
            &mut self.group_state,
            TENANT,
            RulerShard::new(1, 1).expect("one shard of one"),
            eval_time_ms,
        )
        .await
        .expect("the bundle evaluates");
        self.wal.take()
    }

    /// Reads one Prometheus API endpoint of this ruler.
    async fn api_json(&self, uri: &str) -> serde_json::Value {
        let answer = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("X-Scope-OrgID", TENANT)
                    .body(Body::empty())
                    .expect("API request"),
            )
            .await
            .expect("the API answers");
        check!(answer.status() == StatusCode::OK);
        let body = to_bytes(answer.into_body(), 1 << 20)
            .await
            .expect("API body");
        serde_json::from_slice(&body).expect("API JSON")
    }

    /// Returns every alert the API renders at `eval_time_ms`.
    async fn alerts_at(&self, eval_time_ms: i64) -> Vec<RenderedAlert> {
        self.state.set_ruler_evaluation_time_ms(eval_time_ms);
        let json = self.api_json("/prometheus/api/v1/alerts").await;
        let mut alerts = json["data"]["alerts"]
            .as_array()
            .expect("alert array")
            .iter()
            .map(RenderedAlert::from_json)
            .collect::<Vec<_>>();
        alerts.sort();
        alerts
    }

    /// Returns the expanded annotations of each alert, keyed by alert name.
    async fn annotations_at(
        &self,
        eval_time_ms: i64,
    ) -> BTreeMap<String, BTreeMap<String, String>> {
        self.state.set_ruler_evaluation_time_ms(eval_time_ms);
        let json = self.api_json("/prometheus/api/v1/alerts").await;
        json["data"]["alerts"]
            .as_array()
            .expect("alert array")
            .iter()
            .map(|alert| {
                let name = alert["name"].as_str().unwrap_or_default().to_string();
                let annotations = alert["annotations"]
                    .as_object()
                    .map(|object| {
                        object
                            .iter()
                            .map(|(key, value)| {
                                (key.clone(), value.as_str().unwrap_or_default().to_string())
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (name, annotations)
            })
            .collect()
    }

    /// Returns the alerts that fire once every `for` extent has elapsed.
    ///
    /// The first read starts each alert instance, and the second read is late
    /// enough that every hold in the bundle has passed.
    async fn firing_alerts(&self) -> Vec<RenderedAlert> {
        let pending = self.alerts_at(MIDPOINT_MS).await;
        check!(pending.iter().all(|alert| alert.state == "pending"));
        self.alerts_at(END_MS)
            .await
            .into_iter()
            .filter(|alert| alert.state == "firing")
            .collect()
    }
}

/// One alert instance as `/api/v1/alerts` renders it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RenderedAlert {
    name: String,
    state: String,
    severity: String,
    node: String,
    value: String,
}

impl RenderedAlert {
    fn from_json(alert: &serde_json::Value) -> Self {
        let label = |name: &str| {
            alert["labels"][name]
                .as_str()
                .unwrap_or_default()
                .to_string()
        };
        Self {
            name: alert["name"].as_str().unwrap_or_default().to_string(),
            state: alert["state"].as_str().unwrap_or_default().to_string(),
            severity: label("severity"),
            node: label("node"),
            value: alert["value"].as_str().unwrap_or_default().to_string(),
        }
    }

    fn firing(name: &str, severity: &str, node: &str, value: &str) -> Self {
        Self {
            name: name.to_string(),
            state: "firing".to_string(),
            severity: severity.to_string(),
            node: node.to_string(),
            value: value.to_string(),
        }
    }

    /// One firing alert whose sample value comes from an extrapolation over a
    /// range, where only the identity of the alert matters.
    fn firing_any_value(name: &str, severity: &str, node: &str) -> Self {
        Self::firing(name, severity, node, ANY_VALUE)
    }

    fn without_value(mut self) -> Self {
        self.value = ANY_VALUE.to_string();
        self
    }

    fn pending(name: &str, severity: &str, node: &str, value: &str) -> Self {
        Self {
            state: "pending".to_string(),
            ..Self::firing(name, severity, node, value)
        }
    }
}

#[derive(Clone, Default)]
struct CapturedRecordings(Arc<Mutex<Vec<WalRecord>>>);

impl CapturedRecordings {
    fn take(&self) -> Vec<WalRecord> {
        std::mem::take(&mut *self.0.lock().expect("recording sink"))
    }
}

#[async_trait::async_trait]
impl RecordingRuleWalSink for CapturedRecordings {
    async fn append_recording_rule_record(&self, record: WalRecord) -> Result<(), RulerWalError> {
        self.0.lock().expect("recording sink").push(record);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct CapturedAlerts(Arc<Mutex<Vec<AlertmanagerAlert>>>);

impl CapturedAlerts {
    fn take(&self) -> Vec<AlertmanagerAlert> {
        std::mem::take(&mut *self.0.lock().expect("alert sink"))
    }
}

#[async_trait::async_trait]
impl AlertmanagerSink for CapturedAlerts {
    async fn dispatch_alerts(&self, alerts: Vec<AlertmanagerAlert>) -> Result<(), RulerWalError> {
        self.0.lock().expect("alert sink").extend(alerts);
        Ok(())
    }
}

struct NoRulerState;

#[async_trait::async_trait]
impl RulerStateSink for NoRulerState {
    async fn persist_ruler_group_state(
        &self,
        _record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        Ok(())
    }

    async fn persist_ruler_alert_state(
        &self,
        _record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        Ok(())
    }
}

/// Finds the shipped bundle from the working directory of the test.
///
/// Cargo runs an integration test from the crate directory. Bazel runs it from
/// the runfiles root, where a `data` file keeps its own workspace path.
fn bundle_path() -> PathBuf {
    [
        "rules/krabka-clock.yaml",
        "crates/metrics-service/rules/krabka-clock.yaml",
    ]
    .into_iter()
    .map(PathBuf::from)
    .find(|candidate| candidate.is_file())
    .expect("the shipped rule bundle is on the data path of the test")
}

/// Renders one label set as `name{label="value",…}`.
fn series_key(labels: &Labels) -> String {
    let name = labels.get("__name__").unwrap_or_default().to_string();
    let rest = labels
        .iter()
        .filter(|(label, _)| label.as_str() != "__name__")
        .map(|(label, value)| format!("{label}=\"{value}\""))
        .collect::<Vec<_>>()
        .join(",");
    format!("{name}{{{rest}}}")
}

fn float_value(record: &WalRecord) -> f64 {
    match record.payload {
        SamplePayload::Float { value, .. } => value,
        _ => panic!("every recording rule of this bundle records a float sample"),
    }
}

/// The placeholder that stands for a sample value an extrapolation produces.
const ANY_VALUE: &str = "<any value>";

/// Renders a sample value the way `/api/v1/alerts` renders it.
///
/// Every magnitude in this suite sits between `1e-6` and `1e21`, which is the
/// range Prometheus writes as a plain decimal.
fn sample_text(value: f64) -> String {
    format!("{value}")
}

fn max_of(values: impl Iterator<Item = f64>) -> f64 {
    values.fold(f64::NEG_INFINITY, f64::max)
}

fn min_of(values: impl Iterator<Item = f64>) -> f64 {
    values.fold(f64::INFINITY, f64::min)
}

/// The value the fleet-skew recording rule derives from a fleet.
fn fleet_skew_seconds(fleet: &[ClockFixture]) -> f64 {
    let upper = max_of(
        fleet
            .iter()
            .map(|clock| clock.offset_seconds() + clock.uncertainty_seconds()),
    );
    let lower = min_of(
        fleet
            .iter()
            .map(|clock| clock.offset_seconds() - clock.uncertainty_seconds()),
    );
    upper - lower
}

fn widest_uncertainty_seconds(fleet: &[ClockFixture]) -> f64 {
    max_of(fleet.iter().map(|clock| clock.uncertainty_seconds()))
}

fn declared_bound_seconds() -> f64 {
    max_of(
        DECLARED_BOUND_STEPS
            .iter()
            .map(|steps| ClockFixture::seconds(*steps)),
    )
}

fn sample_timestamp(record: &WalRecord) -> i64 {
    match record.payload {
        SamplePayload::Float { timestamp_ms, .. } => timestamp_ms,
        _ => panic!("every recording rule of this bundle records a float sample"),
    }
}

fn recorded_values(records: &[WalRecord]) -> BTreeMap<String, f64> {
    records
        .iter()
        .map(|record| (series_key(&record.labels()), float_value(record)))
        .collect()
}

/// Builds a store that holds the clock projection and the series the recording
/// rules derive from it.
///
/// The ruler writes a recorded series to the WAL, and the store reads it back
/// on a later cycle. An alert that reads a recorded series therefore sees it
/// one interval later, and the budget ratio needs one cycle more than that
/// because it reads two recorded series itself. These four cycles give every
/// rule of the bundle an input inside the lookback window of the two
/// evaluation times this suite reads at.
async fn store_with_derived_series(fleet: &[ClockFixture]) -> InMemoryMetricStore {
    let mut store = store_with_fleet(fleet, &DECLARED_BOUND_STEPS);
    for eval_time_ms in [
        MIDPOINT_MS - READING_INTERVAL_MS,
        MIDPOINT_MS,
        END_MS - READING_INTERVAL_MS,
        END_MS,
    ] {
        let mut ruler = Ruler::with_store(store.clone()).await;
        for record in ruler.evaluate(eval_time_ms).await {
            store.push_float(
                TENANT,
                record.labels(),
                sample_timestamp(&record),
                float_value(&record),
            );
        }
    }
    store
}

#[tokio::test]
async fn the_bundle_installs_as_two_groups_of_one_namespace() {
    let ruler = Ruler::with_fleet(&healthy_fleet()).await;

    let installed = ruler
        .state
        .ruler_rule_set(TENANT)
        .into_iter()
        .map(|(namespace, groups)| (namespace, groups.into_keys().collect::<Vec<_>>()))
        .collect::<BTreeMap<_, _>>();

    check!(
        installed
            == BTreeMap::from([(
                "krabka-clock".to_string(),
                vec![
                    "krabka-clock".to_string(),
                    "krabka-clock-alerts".to_string(),
                ],
            )])
    );
}

#[tokio::test]
async fn every_bundled_rule_evaluates_against_the_engine() {
    let ruler = Ruler::with_fleet(&healthy_fleet()).await;
    ruler.state.set_ruler_evaluation_time_ms(MIDPOINT_MS);

    let answer = ruler.api_json("/prometheus/api/v1/rules").await;
    let rendered = answer["data"]["groups"]
        .as_array()
        .expect("rule groups")
        .iter()
        .flat_map(|group| group["rules"].as_array().expect("rules").iter())
        .map(|rule| {
            (
                rule["name"].as_str().unwrap_or_default().to_string(),
                rule["type"].as_str().unwrap_or_default().to_string(),
                rule["health"].as_str().unwrap_or_default().to_string(),
                rule["lastError"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect::<Vec<_>>();

    // A rule whose expression the engine cannot evaluate renders as `err` and
    // carries the failure in `lastError`.
    let unhealthy = rendered
        .iter()
        .filter(|(_, _, health, _)| health != "ok")
        .collect::<Vec<_>>();
    check!(unhealthy == Vec::<&(String, String, String, String)>::new());
    check!(rendered.len() == 14);
}

#[tokio::test]
async fn the_recording_rules_derive_the_fleet_clock_signal() {
    let fleet = healthy_fleet();
    let mut ruler = Ruler::with_fleet(&fleet).await;

    let recorded = recorded_values(&ruler.evaluate(MIDPOINT_MS).await);

    // The budget ratio reads two recorded series, and this first cycle is the
    // one that writes them, so it records nothing yet.
    check!(
        recorded
            == BTreeMap::from([
                (
                    "krabka_clock:declared_bound_seconds{}".to_string(),
                    declared_bound_seconds(),
                ),
                (
                    "krabka_clock:uncertainty_seconds:max{}".to_string(),
                    widest_uncertainty_seconds(&fleet),
                ),
                (
                    "krabka_clock:fleet_skew_bound_seconds{}".to_string(),
                    fleet_skew_seconds(&fleet),
                ),
                ("krabka_clock:unsynchronized_nodes{}".to_string(), 0.0),
            ])
    );
}

#[tokio::test]
async fn the_fleet_skew_bound_spans_the_widest_pair_of_clock_intervals() {
    // node-a claims [3, 5] steps, node-b claims [-3, -1], and node-c claims
    // [-3, 3]. The widest pair is the upper end of node-a against the lower end
    // of node-b and node-c, which is eight steps.
    let fleet = healthy_fleet();
    let mut ruler = Ruler::with_fleet(&fleet).await;

    let recorded = recorded_values(&ruler.evaluate(MIDPOINT_MS).await);

    check!(
        recorded.get("krabka_clock:fleet_skew_bound_seconds{}") == Some(&ClockFixture::seconds(8))
    );
}

#[tokio::test]
async fn the_budget_ratio_follows_the_recorded_inputs_by_one_cycle() {
    let fleet = healthy_fleet();
    let mut first = Ruler::with_fleet(&fleet).await;
    let mut store = store_with_fleet(&fleet, &DECLARED_BOUND_STEPS);
    for record in first.evaluate(MIDPOINT_MS - READING_INTERVAL_MS).await {
        store.push_float(
            TENANT,
            record.labels(),
            sample_timestamp(&record),
            float_value(&record),
        );
    }

    let mut second = Ruler::with_store(store).await;
    let recorded = recorded_values(&second.evaluate(MIDPOINT_MS).await);

    check!(
        recorded.get("krabka_clock:uncertainty_budget_ratio{}")
            == Some(&(widest_uncertainty_seconds(&fleet) / declared_bound_seconds()))
    );
}

#[tokio::test]
async fn the_unsynchronized_count_counts_the_clocks_that_track_no_reference() {
    let cases = [
        ("every clock synchronized", vec![], 0.0),
        ("one clock in holdover", vec![ClockSyncState::Holdover], 1.0),
        (
            "one clock in holdover and one free running",
            vec![ClockSyncState::Holdover, ClockSyncState::FreeRunning],
            2.0,
        ),
    ];

    for (name, degraded, expected) in cases {
        let mut fleet = healthy_fleet();
        for (clock, state) in fleet.iter_mut().zip(degraded) {
            *clock = clock.with_sync_state(state);
        }
        let mut ruler = Ruler::with_fleet(&fleet).await;

        let recorded = recorded_values(&ruler.evaluate(MIDPOINT_MS).await);

        check!(
            recorded.get("krabka_clock:unsynchronized_nodes{}") == Some(&expected),
            "{name}"
        );
    }
}

#[tokio::test]
async fn no_alert_fires_for_a_fleet_that_keeps_its_promise() {
    let ruler = Ruler::with_derived_fleet(&healthy_fleet()).await;

    check!(ruler.alerts_at(END_MS).await == Vec::<RenderedAlert>::new());
}

#[tokio::test]
async fn the_uncertainty_alerts_cross_their_thresholds() {
    // The comparisons are strict, and a clock whose uncertainty passes the
    // declared bound also widens the fleet skew past twice that bound, because
    // its own interval is already wider than two bounds.
    let cases = [
        ("one step of uncertainty, far under the bound", 1),
        ("half of the declared bound, and no alert", 3),
        ("more than half of the bound, and a warning", 4),
        ("the declared bound exactly, which is not above it", 6),
        ("one step over the declared bound", 7),
    ];

    for (name, uncertainty_steps) in cases {
        let mut fleet = healthy_fleet();
        fleet[2] = ClockFixture::healthy("node-c", 0, uncertainty_steps);
        let ruler = Ruler::with_derived_fleet(&fleet).await;
        let widest = widest_uncertainty_seconds(&fleet);
        let ratio = widest / declared_bound_seconds();

        let mut expected = Vec::new();
        if fleet_skew_seconds(&fleet) > 2.0 * declared_bound_seconds() {
            expected.push(RenderedAlert::firing(
                "ClockFleetSkewExceedsDeclaredBound",
                "critical",
                "",
                &sample_text(fleet_skew_seconds(&fleet)),
            ));
        }
        if ratio > 0.5 {
            expected.push(RenderedAlert::firing(
                "ClockUncertaintyBudgetHigh",
                "warning",
                "",
                &sample_text(ratio),
            ));
        }
        if widest > declared_bound_seconds() {
            expected.push(RenderedAlert::firing(
                "ClockUncertaintyExceedsDeclaredBound",
                "critical",
                "node-c",
                &sample_text(widest),
            ));
        }

        check!(ruler.firing_alerts().await == expected, "{name}");
    }
}

#[tokio::test]
async fn the_uncertainty_alert_pends_until_its_hold_elapses() {
    let mut fleet = healthy_fleet();
    fleet[2] = ClockFixture::healthy("node-c", 0, 7);
    let ruler = Ruler::with_derived_fleet(&fleet).await;
    let value = sample_text(widest_uncertainty_seconds(&fleet));
    let held_for_ms = 2 * 60_000;

    let start = state_of(
        &ruler.alerts_at(MIDPOINT_MS).await,
        "ClockUncertaintyExceedsDeclaredBound",
    );
    let just_before = state_of(
        &ruler.alerts_at(MIDPOINT_MS + held_for_ms - 1_000).await,
        "ClockUncertaintyExceedsDeclaredBound",
    );
    let on_the_hold = state_of(
        &ruler.alerts_at(MIDPOINT_MS + held_for_ms).await,
        "ClockUncertaintyExceedsDeclaredBound",
    );

    check!(
        start
            == Some(RenderedAlert::pending(
                "ClockUncertaintyExceedsDeclaredBound",
                "critical",
                "node-c",
                &value,
            ))
    );
    check!(just_before == start);
    check!(
        on_the_hold
            == Some(RenderedAlert::firing(
                "ClockUncertaintyExceedsDeclaredBound",
                "critical",
                "node-c",
                &value,
            ))
    );
}

#[tokio::test]
async fn a_clock_under_the_declared_bound_never_starts_the_alert() {
    let ruler = Ruler::with_derived_fleet(&healthy_fleet()).await;

    for eval_time_ms in [MIDPOINT_MS, MIDPOINT_MS + 2 * 60_000, END_MS] {
        let alerts = ruler.alerts_at(eval_time_ms).await;

        check!(state_of(&alerts, "ClockUncertaintyExceedsDeclaredBound") == None);
    }
}

#[tokio::test]
async fn the_stale_alert_fires_when_no_reading_is_present() {
    let ruler = Ruler::with_store(InMemoryMetricStore::new()).await;

    let firing = ruler.firing_alerts().await;

    check!(
        firing
            == vec![RenderedAlert::firing(
                "ClockTelemetryStale",
                "critical",
                "",
                "1"
            )]
    );
}

#[tokio::test]
async fn the_stale_alert_names_the_clock_that_stopped_reporting() {
    // node-a reports ten readings and then goes silent. node-b keeps
    // reporting, so the fleet as a whole is not absent.
    let last_reading_ms = START_MS + 9 * READING_INTERVAL_MS;
    let fleet = [
        ClockFixture::healthy("node-a", 0, 1).stopping_after(last_reading_ms),
        ClockFixture::healthy("node-b", 0, 1),
    ];
    let ruler = Ruler::with_fleet(&fleet).await;
    let readings = "10";

    let pending = ruler.alerts_at(END_MS - 6 * 60_000).await;
    let firing = ruler.alerts_at(END_MS).await;

    check!(
        pending
            == vec![RenderedAlert::pending(
                "ClockTelemetryStale",
                "critical",
                "node-a",
                readings,
            )]
    );
    check!(
        firing
            == vec![RenderedAlert::firing(
                "ClockTelemetryStale",
                "critical",
                "node-a",
                readings,
            )]
    );
}

#[tokio::test]
async fn the_state_alerts_pick_the_current_state() {
    // Every reading publishes all five states, the current one at 1 and the
    // rest at 0. An alert that matched a zero-valued sibling would fire on
    // every state at once.
    let cases = [
        (ClockSyncState::Synchronized, Vec::new()),
        (
            ClockSyncState::Holdover,
            vec![RenderedAlert::firing(
                "ClockInHoldover",
                "warning",
                "node-a",
                "1",
            )],
        ),
        (
            ClockSyncState::FreeRunning,
            vec![RenderedAlert::firing(
                "ClockUnsynchronized",
                "critical",
                "node-a",
                "1",
            )],
        ),
        (
            ClockSyncState::Unsynchronized,
            vec![RenderedAlert::firing(
                "ClockUnsynchronized",
                "critical",
                "node-a",
                "1",
            )],
        ),
        (ClockSyncState::Stepped, Vec::new()),
    ];

    for (state, expected) in cases {
        let fleet = [ClockFixture::healthy("node-a", 0, 1).with_sync_state(state)];
        let ruler = Ruler::with_derived_fleet(&fleet).await;

        check!(ruler.firing_alerts().await == expected, "{state:?}");
    }
}

#[tokio::test]
async fn the_gnss_alert_fires_only_while_the_receiver_reports_no_fix() {
    let cases = [
        (GnssFix::ThreeD, Vec::new()),
        (GnssFix::TwoD, Vec::new()),
        (
            GnssFix::None,
            vec![RenderedAlert::firing(
                "GnssFixLost",
                "warning",
                "node-a",
                "1",
            )],
        ),
    ];

    for (fix, expected) in cases {
        let fleet = [ClockFixture::healthy("node-a", 0, 1).with_gnss_fix(fix)];
        let ruler = Ruler::with_derived_fleet(&fleet).await;

        check!(ruler.firing_alerts().await == expected, "{fix:?}");
    }
}

#[tokio::test]
async fn the_step_and_grandmaster_alerts_read_their_ranges() {
    let stepping = [ClockFixture::healthy("node-a", 0, 1).with_clock_steps(1)];
    let flapping = [ClockFixture::healthy("node-b", 0, 1).with_flapping_clock_class(6)];

    let stepped = Ruler::with_derived_fleet(&stepping)
        .await
        .firing_alerts()
        .await;
    let flapped = Ruler::with_derived_fleet(&flapping)
        .await
        .firing_alerts()
        .await;

    // The step total and the advertised class both come from a range, and
    // `increase()` extrapolates over the window, so only the identity of the
    // alert is fixed.
    check!(
        stepped
            .into_iter()
            .map(RenderedAlert::without_value)
            .collect::<Vec<_>>()
            == vec![RenderedAlert::firing_any_value(
                "ClockStepped",
                "critical",
                "node-a",
            )]
    );
    check!(
        flapped
            .into_iter()
            .map(RenderedAlert::without_value)
            .collect::<Vec<_>>()
            == vec![RenderedAlert::firing_any_value(
                "PtpGrandmasterFlapping",
                "warning",
                "node-b",
            )]
    );
}

#[tokio::test]
async fn a_firing_alert_names_its_node_and_its_value() {
    let mut fleet = healthy_fleet();
    fleet[2] = ClockFixture::healthy("node-c", 0, 7).with_sync_state(ClockSyncState::Holdover);
    let ruler = Ruler::with_derived_fleet(&fleet).await;
    ruler.state.set_ruler_evaluation_time_ms(END_MS);

    let annotations = ruler.annotations_at(END_MS).await;

    let holdover = &annotations["ClockInHoldover"];
    check!(holdover["summary"].contains("node-c"));
    check!(holdover["description"].contains("node-c"));
    let uncertainty = &annotations["ClockUncertaintyExceedsDeclaredBound"];
    check!(uncertainty["summary"].contains("node-c"));
    check!(uncertainty["description"].contains(&sample_text(ClockFixture::seconds(7))));
}

#[tokio::test]
async fn the_ruler_dispatches_the_firing_clock_alerts() {
    let fleet = [ClockFixture::healthy("node-a", 0, 1).with_sync_state(ClockSyncState::Holdover)];
    let mut ruler = Ruler::with_derived_fleet(&fleet).await;

    ruler.evaluate(MIDPOINT_MS).await;
    let pending = ruler.alerts.take();
    ruler.evaluate(END_MS).await;
    let firing = ruler
        .alerts
        .take()
        .iter()
        .map(DispatchedAlert::from_alert)
        .collect::<Vec<_>>();

    // The hold of `ClockInHoldover` is ten minutes, so the first evaluation
    // only starts the instance and dispatches nothing.
    check!(pending == Vec::<AlertmanagerAlert>::new());
    check!(
        firing
            == vec![DispatchedAlert {
                labels: BTreeMap::from([
                    (
                        "__name__".to_string(),
                        "krabka_clock_sync_state".to_string()
                    ),
                    ("alertname".to_string(), "ClockInHoldover".to_string()),
                    ("clock".to_string(), "CLOCK_REALTIME".to_string()),
                    ("node".to_string(), "node-a".to_string()),
                    ("severity".to_string(), "warning".to_string()),
                    ("source".to_string(), "ptp".to_string()),
                    ("state".to_string(), "holdover".to_string()),
                ]),
                starts_at_ms: MIDPOINT_MS,
                ends_at_ms: None,
            }]
    );
}

/// One dispatched alert, without the annotations its rule expands.
#[derive(Debug, Eq, PartialEq)]
struct DispatchedAlert {
    labels: BTreeMap<String, String>,
    starts_at_ms: i64,
    ends_at_ms: Option<i64>,
}

impl DispatchedAlert {
    fn from_alert(alert: &AlertmanagerAlert) -> Self {
        Self {
            labels: alert.labels.clone(),
            starts_at_ms: alert.starts_at_ms,
            ends_at_ms: alert.ends_at_ms,
        }
    }
}

fn state_of(alerts: &[RenderedAlert], name: &str) -> Option<RenderedAlert> {
    alerts.iter().find(|alert| alert.name == name).cloned()
}
