use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use assert2::check;
use krabka_blockstore::Labels;
use krabka_metrics::{SamplePayload, WalRecord};
use krabka_units::prelude::*;

use crate::{
    EngineOpts, InMemoryMetricStore, PromqlEngine, RulerGroupEvaluation, test_support::tenant_id,
};

mod byte_alert_templates_and_replayed_state_preserve_series_identity;
mod recording_alertmanager_sink;
mod recording_ruler_state_sink;
mod recording_sink;
mod rule_fixtures;
mod ruler_failure_isolation_and_alert_series;

mod alerting_dispatch;
mod recording_rule_cases;
mod rule_set_scheduling;
use labels::labels;
use recording_alertmanager_sink::RecordingAlertmanagerSink;
use recording_ruler_state_sink::RecordingRulerStateSink;
use recording_sink::RecordingSink;
use rule_fixtures::{
    GroupLastEval, UpRuleSinks, UpSample, assert_up_rules_fired_at_six_minutes, group_state,
    instance_up_alert, job_up_current_record, mixed_rule_group, namespaced_rule_set,
    staggered_group_state, standard_due_interval_rule_set, two_job_up_engine, up_alert_rule,
    up_api_engine, up_samples_engine, up_transition_engine,
};

#[derive(Default)]
struct RecordingRulerSinks {
    wal: RecordingSink,
    alertmanager: RecordingAlertmanagerSink,
    ruler_state: RecordingRulerStateSink,
}

struct RecordingRulerSinksAndState {
    sinks: RecordingRulerSinks,
    alert_state: super::RulerAlertState,
}

fn recording_sinks_and_alert_state() -> RecordingRulerSinksAndState {
    RecordingRulerSinksAndState {
        sinks: RecordingRulerSinks::default(),
        alert_state: super::RulerAlertState::default(),
    }
}

#[derive(bon::Builder)]
struct RulerEvaluationFixture {
    engine: PromqlEngine<InMemoryMetricStore>,
    wal_sink: RecordingSink,
    alert_sink: RecordingAlertmanagerSink,
    state: super::RulerAlertState,
}

impl RulerEvaluationFixture {
    fn up_api_pair() -> Self {
        Self::builder()
            .engine(up_api_engine(&[60_000, 360_000]))
            .wal_sink(RecordingSink::default())
            .alert_sink(RecordingAlertmanagerSink::default())
            .state(super::RulerAlertState::default())
            .build()
    }
}

fn assert_pending_rule_evaluation(pending: &RulerGroupEvaluation) {
    assert2::assert!(
        pending
            == &RulerGroupEvaluation {
                recording_records: 1,
                alerts_dispatched: 0,
                last_eval_ms: 60_000,
            }
    );
}

mod support;
pub(crate) use support::{
    labels, parse_duration_supports_all_units_and_compounds_and_rejects_bad_input,
};
