//! Bounded model of concurrent index-snapshot publication and payload sweep.
//!
//! This mirrors `put_manifest_snapshot` and `sweep_orphan_shard_payloads`:
//! writers read a generation, write an immutable content-addressed payload,
//! and conditionally create the next manifest. A loser re-reads and merges;
//! the sweep deletes only old payloads that no manifest or active writer needs.

use std::time::Duration;

use stateright::{Checker, Model, Property};

const MAX_DEPTH: usize = 32;
const MAX_STATES: usize = 100_000;
const SNAPSHOT_STATES: usize = 580;
const CONFLICT: u8 = 1;
const CRASH: u8 = 2;
const RECLAIM: u8 = 4;
const REPLAY: u8 = 8;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum WriterPhase {
    Pending,
    Read { base: u8, merged: u8 },
    Publish { base: u8, merged: u8 },
    Done,
    Abandoned,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SnapshotState {
    generation: u8,
    latest: u8,
    payloads: u8,
    young: u8,
    writers: [WriterPhase; 2],
    events: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Action {
    Read(usize),
    Write(usize),
    Publish(usize),
    Crash(usize),
    Restart(usize),
    Age(u8),
    Sweep,
}

struct SnapshotModel;

fn contribution(writer: usize) -> u8 {
    1 << writer
}

fn payload_bit(content: u8) -> u8 {
    1 << content
}

fn has_payload(state: &SnapshotState, content: u8) -> bool {
    state.payloads & payload_bit(content) != 0
}

fn writer_needs(state: &SnapshotState, content: u8) -> bool {
    state
        .writers
        .iter()
        .any(|phase| matches!(phase, WriterPhase::Publish { merged, .. } if *merged == content))
}

impl Model for SnapshotModel {
    type State = SnapshotState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![SnapshotState {
            generation: 0,
            latest: 0,
            payloads: 0,
            young: 0,
            writers: [WriterPhase::Pending, WriterPhase::Pending],
            events: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for (writer, phase) in state.writers.iter().enumerate() {
            match phase {
                WriterPhase::Pending => actions.push(Action::Read(writer)),
                WriterPhase::Read { .. } => actions.push(Action::Write(writer)),
                WriterPhase::Publish { .. } => actions.push(Action::Publish(writer)),
                WriterPhase::Abandoned => actions.push(Action::Restart(writer)),
                WriterPhase::Done => {}
            }
            if !matches!(phase, WriterPhase::Done | WriterPhase::Abandoned) {
                actions.push(Action::Crash(writer));
            }
        }

        for content in 1_u8..=3 {
            let bit = payload_bit(content);
            if state.young & bit != 0 && state.latest != content && !writer_needs(state, content) {
                actions.push(Action::Age(content));
            }
        }
        let referenced = payload_bit(state.latest);
        if state.payloads & !state.young & !referenced != 0 {
            actions.push(Action::Sweep);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Read(writer) if matches!(state.writers[writer], WriterPhase::Pending) => {
                state.writers[writer] = WriterPhase::Read {
                    base: state.generation,
                    merged: state.latest | contribution(writer),
                };
            }
            Action::Write(writer) => {
                let WriterPhase::Read { base, merged } = state.writers[writer] else {
                    return None;
                };
                let bit = payload_bit(merged);
                state.payloads |= bit;
                state.young |= bit;
                state.writers[writer] = WriterPhase::Publish { base, merged };
            }
            Action::Publish(writer) => {
                let WriterPhase::Publish { base, merged } = state.writers[writer] else {
                    return None;
                };
                if base == state.generation {
                    state.generation += 1;
                    state.latest = merged;
                    state.young &= !payload_bit(merged);
                    state.writers[writer] = WriterPhase::Done;
                } else {
                    state.events |= CONFLICT;
                    state.writers[writer] = WriterPhase::Pending;
                }
            }
            Action::Crash(writer)
                if !matches!(
                    state.writers[writer],
                    WriterPhase::Done | WriterPhase::Abandoned
                ) =>
            {
                state.events |= CRASH;
                state.writers[writer] = WriterPhase::Abandoned;
            }
            Action::Restart(writer) if matches!(state.writers[writer], WriterPhase::Abandoned) => {
                state.events |= REPLAY;
                state.writers[writer] = WriterPhase::Pending;
            }
            Action::Age(content)
                if state.young & payload_bit(content) != 0
                    && state.latest != content
                    && !writer_needs(&state, content) =>
            {
                state.young &= !payload_bit(content);
            }
            Action::Sweep => {
                let referenced = payload_bit(state.latest);
                let garbage = state.payloads & !state.young & !referenced;
                if garbage == 0 {
                    return None;
                }
                state.payloads &= !garbage;
                state.events |= RECLAIM;
            }
            Action::Read(_) | Action::Crash(_) | Action::Restart(_) | Action::Age(_) => {
                return None;
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "published manifests reference durable payloads",
                |_, state: &SnapshotState| state.latest == 0 || has_payload(state, state.latest),
            ),
            Property::always(
                "successful writers are never lost",
                |_, state: &SnapshotState| {
                    state.writers.iter().enumerate().all(|(writer, phase)| {
                        !matches!(phase, WriterPhase::Done)
                            || state.latest & contribution(writer) != 0
                    })
                },
            ),
            Property::always(
                "active publications retain their payload",
                |_, state: &SnapshotState| {
                    state.writers.iter().all(|phase| match phase {
                        WriterPhase::Publish { merged, .. } => has_payload(state, *merged),
                        _ => true,
                    })
                },
            ),
            Property::sometimes("conflicting writers merge", |_, state: &SnapshotState| {
                state.events & CONFLICT != 0
                    && state.latest == 3
                    && state
                        .writers
                        .iter()
                        .all(|phase| matches!(phase, WriterPhase::Done))
            }),
            Property::sometimes("crashed writers replay", |_, state: &SnapshotState| {
                state.events & CRASH != 0 && state.events & REPLAY != 0 && state.latest == 3
            }),
            Property::sometimes(
                "orphan payloads are reclaimed",
                |_, state: &SnapshotState| state.events & RECLAIM != 0,
            ),
        ]
    }
}

#[test]
fn concurrent_snapshot_publication_is_safe() {
    let checker = SnapshotModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(Duration::from_secs(30))
        .spawn_bfs()
        .join();
    assert2::check!(checker.max_depth() < MAX_DEPTH);
    assert2::check!(checker.state_count() < MAX_STATES);
    assert2::check!(checker.unique_state_count() == SNAPSHOT_STATES);
    checker.assert_properties();
}
