//! Bounded model of concurrent index-snapshot publication and payload sweep.
//!
//! One writer removes an existing record and one adds a record. Writers read a
//! generation, write an immutable content-addressed payload, and conditionally
//! create the next manifest. Publication has a bounded lease, acknowledgement
//! is separate from manifest creation, and sweep selection is separate from a
//! version-conditional reclamation.

use std::time::Duration;

use stateright::{Checker, Model, Property};

const MAX_DEPTH: usize = 48;
const MAX_STATES: usize = 1_000_000;
const SNAPSHOT_STATES: usize = 113_426;
const CONFLICT: u8 = 1;
const CRASH: u8 = 2;
const RECLAIM: u8 = 4;
const REPLAY: u8 = 8;
const EXPIRED: u8 = 16;
const REWROTE_SELECTED: u8 = 32;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum WriterPhase {
    Pending,
    Read { base: u8, merged: u8 },
    Publish { base: u8, merged: u8, expired: bool },
    Published,
    Done,
    Abandoned,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SnapshotState {
    generation: u8,
    retained: [Option<u8>; 2],
    payloads: u8,
    young: u8,
    selected: u8,
    writers: [WriterPhase; 2],
    post_publish_failures: u8,
    events: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Action {
    Read(usize),
    Write(usize),
    Publish(usize),
    Acknowledge(usize),
    Crash(usize),
    Timeout(usize),
    Restart(usize),
    Age(u8),
    Scan,
    Reclaim,
}

struct SnapshotModel;

fn writer_bit(writer: usize) -> u8 {
    1 << writer
}

fn payload_bit(content: u8) -> u8 {
    1 << content
}

fn latest(state: &SnapshotState) -> u8 {
    state.retained[0].expect("the model starts with a manifest")
}

fn apply(writer: usize, base: u8) -> u8 {
    match writer {
        0 => base & !1,
        1 => base | 2,
        _ => unreachable!("the model has two writers"),
    }
}

fn referenced_payloads(state: &SnapshotState) -> u8 {
    state
        .retained
        .iter()
        .flatten()
        .fold(0, |bits, content| bits | payload_bit(*content))
}

fn has_payload(state: &SnapshotState, content: u8) -> bool {
    state.payloads & payload_bit(content) != 0
}

fn writer_effect_holds(state: &SnapshotState, writer: usize) -> bool {
    match writer {
        0 => latest(state) & 1 == 0,
        1 => latest(state) & 2 != 0,
        _ => unreachable!("the model has two writers"),
    }
}

impl Model for SnapshotModel {
    type State = SnapshotState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![SnapshotState {
            generation: 0,
            retained: [Some(1), None],
            payloads: payload_bit(1),
            young: 0,
            selected: 0,
            writers: [WriterPhase::Pending, WriterPhase::Pending],
            post_publish_failures: 0,
            events: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for (writer, phase) in state.writers.iter().enumerate() {
            match phase {
                WriterPhase::Pending => {
                    actions.push(Action::Read(writer));
                    actions.push(Action::Crash(writer));
                }
                WriterPhase::Read { .. } => {
                    actions.push(Action::Write(writer));
                    actions.push(Action::Crash(writer));
                }
                WriterPhase::Publish { expired: false, .. } => {
                    actions.push(Action::Publish(writer));
                    actions.push(Action::Crash(writer));
                }
                WriterPhase::Publish { expired: true, .. } => {
                    actions.push(Action::Timeout(writer));
                }
                WriterPhase::Published => {
                    actions.push(Action::Acknowledge(writer));
                    if state.post_publish_failures & writer_bit(writer) == 0 {
                        actions.push(Action::Crash(writer));
                    }
                }
                WriterPhase::Abandoned => actions.push(Action::Restart(writer)),
                WriterPhase::Done => {}
            }
        }

        for content in 0_u8..=3 {
            if state.young & payload_bit(content) != 0
                && referenced_payloads(state) & payload_bit(content) == 0
            {
                actions.push(Action::Age(content));
            }
        }
        let garbage = state.payloads & !state.young & !referenced_payloads(state);
        if garbage != 0 {
            actions.push(Action::Scan);
        }
        if state.selected & garbage != 0 {
            actions.push(Action::Reclaim);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Read(writer) if matches!(state.writers[writer], WriterPhase::Pending) => {
                state.writers[writer] = WriterPhase::Read {
                    base: state.generation,
                    merged: apply(writer, latest(&state)),
                };
            }
            Action::Write(writer) => {
                let WriterPhase::Read { base, merged } = state.writers[writer] else {
                    return None;
                };
                let bit = payload_bit(merged);
                if state.selected & bit != 0 {
                    state.events |= REWROTE_SELECTED;
                }
                state.payloads |= bit;
                state.young |= bit;
                state.selected &= !bit;
                state.writers[writer] = WriterPhase::Publish {
                    base,
                    merged,
                    expired: false,
                };
            }
            Action::Publish(writer) => {
                let WriterPhase::Publish {
                    base,
                    merged,
                    expired: false,
                } = state.writers[writer]
                else {
                    return None;
                };
                if base == state.generation {
                    state.generation += 1;
                    state.retained = [Some(merged), state.retained[0]];
                    state.young &= !payload_bit(merged);
                    state.writers[writer] = WriterPhase::Published;
                } else {
                    state.events |= CONFLICT;
                    state.writers[writer] = WriterPhase::Pending;
                }
            }
            Action::Acknowledge(writer)
                if matches!(state.writers[writer], WriterPhase::Published) =>
            {
                state.writers[writer] = WriterPhase::Done;
            }
            Action::Crash(writer) => match state.writers[writer] {
                WriterPhase::Pending | WriterPhase::Read { .. } | WriterPhase::Publish { .. } => {
                    state.events |= CRASH;
                    state.writers[writer] = WriterPhase::Abandoned;
                }
                WriterPhase::Published if state.post_publish_failures & writer_bit(writer) == 0 => {
                    state.events |= CRASH;
                    state.post_publish_failures |= writer_bit(writer);
                    state.writers[writer] = WriterPhase::Abandoned;
                }
                _ => return None,
            },
            Action::Timeout(writer) => {
                let WriterPhase::Publish { expired: true, .. } = state.writers[writer] else {
                    return None;
                };
                state.events |= EXPIRED;
                state.writers[writer] = WriterPhase::Abandoned;
            }
            Action::Restart(writer) if matches!(state.writers[writer], WriterPhase::Abandoned) => {
                state.events |= REPLAY;
                state.writers[writer] = WriterPhase::Pending;
            }
            Action::Age(content)
                if state.young & payload_bit(content) != 0
                    && referenced_payloads(&state) & payload_bit(content) == 0 =>
            {
                state.young &= !payload_bit(content);
                for phase in &mut state.writers {
                    if let WriterPhase::Publish {
                        merged, expired, ..
                    } = phase
                        && *merged == content
                    {
                        *expired = true;
                    }
                }
            }
            Action::Scan => {
                let garbage = state.payloads & !state.young & !referenced_payloads(&state);
                if garbage == 0 {
                    return None;
                }
                state.selected = garbage;
            }
            Action::Reclaim => {
                let garbage = state.payloads & !state.young & !referenced_payloads(&state);
                let reclaim = state.selected & garbage;
                if reclaim == 0 {
                    return None;
                }
                state.payloads &= !reclaim;
                state.selected &= !reclaim;
                state.events |= RECLAIM;
            }
            Action::Read(_) | Action::Acknowledge(_) | Action::Restart(_) | Action::Age(_) => {
                return None;
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "all retained manifests reference durable payloads",
                |_, state: &SnapshotState| {
                    state
                        .retained
                        .iter()
                        .flatten()
                        .all(|content| has_payload(state, *content))
                },
            ),
            Property::always(
                "acknowledgeable writer effects are never lost",
                |_, state: &SnapshotState| {
                    state.writers.iter().enumerate().all(|(writer, phase)| {
                        !matches!(phase, WriterPhase::Published | WriterPhase::Done)
                            || writer_effect_holds(state, writer)
                    })
                },
            ),
            Property::always(
                "unexpired publications retain their payload",
                |_, state: &SnapshotState| {
                    state.writers.iter().all(|phase| match phase {
                        WriterPhase::Publish {
                            merged,
                            expired: false,
                            ..
                        } => has_payload(state, *merged),
                        _ => true,
                    })
                },
            ),
            Property::sometimes(
                "conflicting add and remove writers merge",
                |_, state: &SnapshotState| {
                    state.events & CONFLICT != 0
                        && latest(state) == 2
                        && state
                            .writers
                            .iter()
                            .all(|phase| matches!(phase, WriterPhase::Done))
                },
            ),
            Property::sometimes("expired publications replay", |_, state: &SnapshotState| {
                state.events & EXPIRED != 0 && state.events & REPLAY != 0 && latest(state) == 2
            }),
            Property::sometimes(
                "published removals replay after failure",
                |_, state: &SnapshotState| {
                    state.post_publish_failures & writer_bit(0) != 0
                        && matches!(state.writers[0], WriterPhase::Done)
                        && latest(state) & 1 == 0
                },
            ),
            Property::sometimes(
                "rewrites invalidate stale sweep selections",
                |_, state: &SnapshotState| state.events & REWROTE_SELECTED != 0,
            ),
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
