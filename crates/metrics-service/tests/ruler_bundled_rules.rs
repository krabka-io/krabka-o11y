//! The startup loader that installs a bundled rule file into the ruler.
//!
//! A rule file an operator names and the ruler cannot install is an alerting
//! gap with no signal, so every failure here stops the start.

use std::{path::Path, sync::Arc};

use assert2::check;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use crabka_metrics_service::{
    BundledRulesError, install_bundled_rule_groups, prometheus_api_state_for_store,
};
use crabka_promql::{InMemoryMetricStore, PrometheusApiState, prometheus_router};
use tempfile::TempDir;
use tower::ServiceExt as _;

const TENANT: &str = "tenant-a";

const ONE_GROUP: &str = r#"
groups:
  - name: clock-recording
    interval: 1m
    rules:
      - record: fleet:clock_uncertainty_seconds:max
        expr: max(krabka_clock_uncertainty_seconds)
      - alert: ClockUnsynchronized
        expr: krabka_clock_sync_state{state="unsynchronized"} == 1
        for: 5m
        labels:
          severity: critical
"#;

struct Fixture {
    state: Arc<PrometheusApiState<InMemoryMetricStore>>,
    router: Router,
    dir: TempDir,
}

impl Fixture {
    fn new() -> Self {
        let state = prometheus_api_state_for_store(InMemoryMetricStore::new());
        let router = prometheus_router(Arc::clone(&state));
        Self {
            state,
            router,
            dir: TempDir::new().expect("temporary directory"),
        }
    }

    /// Writes one rule file and returns its path.
    fn write(&self, name: &str, body: &str) -> std::path::PathBuf {
        let path = self.dir.path().join(name);
        std::fs::write(&path, body).expect("rule file");
        path
    }

    async fn install(&self, path: &Path) -> Result<Vec<String>, BundledRulesError> {
        install_bundled_rule_groups(&self.router, path, TENANT).await
    }

    /// Reads the ruler config API back as the operator sees it.
    async fn config_yaml(&self) -> String {
        let answer = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/prometheus/config/v1/rules")
                    .header("X-Scope-OrgID", TENANT)
                    .body(Body::empty())
                    .expect("config request"),
            )
            .await
            .expect("the API answers");
        check!(answer.status() == StatusCode::OK);
        let body = to_bytes(answer.into_body(), 1 << 20)
            .await
            .expect("config body");
        String::from_utf8(body.to_vec()).expect("the config API answers in UTF-8")
    }
}

#[tokio::test]
async fn the_loader_installs_a_group_under_the_file_stem() {
    let fixture = Fixture::new();
    let path = fixture.write("krabka-clock.yaml", ONE_GROUP);

    let installed = fixture.install(&path).await.expect("the file installs");

    check!(installed == vec!["clock-recording".to_string()]);
    let namespaces = fixture
        .state
        .ruler_rule_set(TENANT)
        .into_keys()
        .collect::<Vec<_>>();
    check!(namespaces == vec!["krabka-clock".to_string()]);
    // The group the loader installed reads back through the same API an
    // operator posts to.
    check!(fixture.config_yaml().await.contains("clock-recording"));
}

#[tokio::test]
async fn a_bundled_group_evaluates_like_a_posted_group() {
    let fixture = Fixture::new();
    let bundled = fixture.write("bundled.yaml", ONE_GROUP);
    fixture.install(&bundled).await.expect("the file installs");
    let from_loader = fixture.state.ruler_rule_set(TENANT);

    let posted = Fixture::new();
    let answer =
        posted
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/prometheus/config/v1/rules/bundled")
                    .header("X-Scope-OrgID", TENANT)
                    .header("content-type", "application/yaml")
                    .body(
                        Body::from(
                            serde_yaml::to_string(
                                &serde_yaml::from_str::<serde_yaml::Value>(ONE_GROUP)
                                    .expect("YAML")["groups"][0],
                            )
                            .expect("group YAML"),
                        ),
                    )
                    .expect("post request"),
            )
            .await
            .expect("the API answers");
    check!(answer.status() == StatusCode::ACCEPTED);

    check!(from_loader == posted.state.ruler_rule_set(TENANT));
}

#[tokio::test]
async fn a_missing_file_stops_the_start() {
    let fixture = Fixture::new();
    let path = fixture.dir.path().join("absent.yaml");

    let error = fixture.install(&path).await.expect_err("no such file");

    check!(matches!(error, BundledRulesError::Read { .. }));
    check!(format!("{error}").contains("absent.yaml"));
    check!(fixture.state.ruler_rule_set(TENANT).is_empty());
}

#[tokio::test]
async fn a_file_that_is_not_yaml_stops_the_start() {
    let fixture = Fixture::new();
    let path = fixture.write("broken.yaml", "groups: [ - name: unterminated\n");

    let error = fixture.install(&path).await.expect_err("malformed YAML");

    check!(matches!(error, BundledRulesError::Decode { .. }));
    check!(format!("{error}").contains("broken.yaml"));
    check!(fixture.state.ruler_rule_set(TENANT).is_empty());
}

#[tokio::test]
async fn a_file_with_no_group_stops_the_start() {
    let fixture = Fixture::new();
    let empty = fixture.write("empty.yaml", "groups: []\n");
    // A rule file names its groups under `groups`. A file that carries rules at
    // the top level is a rule group, not a rule file.
    let unwrapped = fixture.write(
        "unwrapped.yaml",
        "name: g\nrules:\n  - record: a:b\n    expr: up\n",
    );

    let no_groups = fixture.install(&empty).await.expect_err("no rule group");
    let not_a_rule_file = fixture
        .install(&unwrapped)
        .await
        .expect_err("not a rule file");

    check!(matches!(no_groups, BundledRulesError::NoGroups { .. }));
    check!(matches!(not_a_rule_file, BundledRulesError::Decode { .. }));
    check!(fixture.state.ruler_rule_set(TENANT).is_empty());
}

#[tokio::test]
async fn a_group_the_config_api_rejects_stops_the_start() {
    let cases = [
        (
            "no name",
            "groups:\n  - rules:\n      - record: a:b\n        expr: up\n",
        ),
        ("no rule", "groups:\n  - name: empty\n    rules: []\n"),
        (
            "a rule that is neither a record nor an alert",
            "groups:\n  - name: neither\n    rules:\n      - expr: up\n",
        ),
        (
            "a rule that is both a record and an alert",
            "groups:\n  - name: both\n    rules:\n      - record: a:b\n        alert: A\n        expr: up\n",
        ),
        (
            "an expression the engine cannot parse",
            "groups:\n  - name: unparseable\n    rules:\n      - record: a:b\n        expr: sum(\n",
        ),
    ];

    for (name, body) in cases {
        let fixture = Fixture::new();
        let path = fixture.write("rejected.yaml", body);

        let error = fixture.install(&path).await.expect_err("rejected group");

        check!(
            matches!(error, BundledRulesError::Rejected { .. }),
            "{name}"
        );
        check!(fixture.state.ruler_rule_set(TENANT).is_empty(), "{name}");
    }
}

#[tokio::test]
async fn a_later_group_that_the_api_rejects_stops_the_start() {
    let fixture = Fixture::new();
    let path = fixture.write(
        "mixed.yaml",
        "groups:\n  - name: good\n    rules:\n      - record: a:b\n        expr: up\n  - name: bad\n    rules: []\n",
    );

    let error = fixture.install(&path).await.expect_err("rejected group");

    // The failure names the group that stopped the start, and not the one
    // before it. The group before it stays in the config store, and the start
    // stops, so the ruler never evaluates either one.
    check!(format!("{error}").contains("`bad`"));
    check!(matches!(error, BundledRulesError::Rejected { .. }));
    let installed = fixture
        .state
        .ruler_rule_set(TENANT)
        .values()
        .map(std::collections::BTreeMap::len)
        .sum::<usize>();
    check!(installed == 1);
}
