//! Bounded model of the metrics compactor's durable publication protocol.
//!
//! This is the ordering implemented by
//! `process_compaction_record_batch`: every partition's block and index are
//! durable before the consumer commits the assignment. Failures and crashes
//! discard only volatile progress; deterministic replay overwrites the same
//! logical publication before trying the commit again.
//!
//! Partition revocation is deliberately outside this model. The production
//! block-builder documents that limitation and issue #266 owns its redesign.

use std::time::Duration;

use stateright::{Checker, Model, Property};

const MAX_DEPTH: usize = 24;
const MAX_STATES: usize = 100_000;
const TWO_PARTITION_STATES: usize = 109;
const THREE_PARTITION_STATES: usize = 174;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Phase {
    Idle,
    Write(usize),
    Publish(usize),
    Commit,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct StorageState {
    phase: Phase,
    durable: Vec<bool>,
    published: Vec<bool>,
    committed: Vec<bool>,
    saw_crash: bool,
    saw_failure: bool,
    saw_replay: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Action {
    Start,
    Write,
    Publish,
    Commit,
    Fail,
    Crash,
}

struct StorageModel {
    partitions: usize,
}

impl Model for StorageModel {
    type State = StorageState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![StorageState {
            phase: Phase::Idle,
            durable: vec![false; self.partitions],
            published: vec![false; self.partitions],
            committed: vec![false; self.partitions],
            saw_crash: false,
            saw_failure: false,
            saw_replay: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        match state.phase {
            Phase::Idle if state.committed.iter().any(|committed| !committed) => {
                actions.push(Action::Start);
            }
            Phase::Write(_) => actions.push(Action::Write),
            Phase::Publish(_) => actions.push(Action::Publish),
            Phase::Commit => actions.push(Action::Commit),
            Phase::Idle => {}
        }
        if !matches!(state.phase, Phase::Idle) {
            actions.push(Action::Fail);
            actions.push(Action::Crash);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Start if matches!(state.phase, Phase::Idle) => {
                state.phase = Phase::Write(0);
            }
            Action::Write => {
                let Phase::Write(partition) = state.phase else {
                    return None;
                };
                state.saw_replay |= state.durable[partition];
                state.durable[partition] = true;
                state.phase = Phase::Publish(partition);
            }
            Action::Publish => {
                let Phase::Publish(partition) = state.phase else {
                    return None;
                };
                state.published[partition] = true;
                state.phase = if partition + 1 == self.partitions {
                    Phase::Commit
                } else {
                    Phase::Write(partition + 1)
                };
            }
            Action::Commit if matches!(state.phase, Phase::Commit) => {
                state.committed.fill(true);
                state.phase = Phase::Idle;
            }
            Action::Fail if !matches!(state.phase, Phase::Idle) => {
                state.saw_failure = true;
                state.phase = Phase::Idle;
            }
            Action::Crash if !matches!(state.phase, Phase::Idle) => {
                state.saw_crash = true;
                state.phase = Phase::Idle;
            }
            Action::Start | Action::Commit | Action::Fail | Action::Crash => return None,
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("published blocks are durable", |_, state: &StorageState| {
                state
                    .published
                    .iter()
                    .zip(&state.durable)
                    .all(|(published, durable)| !published || *durable)
            }),
            Property::always(
                "committed offsets are published",
                |_, state: &StorageState| {
                    state
                        .committed
                        .iter()
                        .zip(&state.published)
                        .all(|(committed, published)| !committed || *published)
                },
            ),
            Property::always(
                "assignment commits atomically",
                |_, state: &StorageState| {
                    state.committed.iter().all(|value| *value)
                        || state.committed.iter().all(|value| !value)
                },
            ),
            Property::sometimes("crash replay commits", |_, state: &StorageState| {
                state.saw_crash && state.committed.iter().all(|value| *value)
            }),
            Property::sometimes("failure replay commits", |_, state: &StorageState| {
                state.saw_failure && state.committed.iter().all(|value| *value)
            }),
            Property::sometimes("durable writes are replayed", |_, state: &StorageState| {
                state.saw_replay && state.committed.iter().all(|value| *value)
            }),
        ]
    }
}

fn check(partitions: usize, expected_states: usize) {
    let checker = StorageModel { partitions }
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(Duration::from_secs(30))
        .spawn_bfs()
        .join();
    assert2::check!(checker.max_depth() < MAX_DEPTH);
    assert2::check!(checker.state_count() < MAX_STATES);
    assert2::check!(checker.unique_state_count() == expected_states);
    checker.assert_properties();
}

#[test]
fn two_partition_protocol_is_safe() {
    check(2, TWO_PARTITION_STATES);
}

#[test]
fn three_partition_protocol_is_safe() {
    check(3, THREE_PARTITION_STATES);
}
