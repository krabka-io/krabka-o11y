//! Bounded model of sidecar-first block deletion.
//!
//! This is the ordering implemented by `krabka_blockstore::delete_blocks`:
//! every sidecar is absent before its block is deleted. Failures and crashes
//! preserve completed deletes, and a later sweep safely replays the sequence.

use std::time::Duration;

use stateright::{Checker, Model, Property};

const MAX_DEPTH: usize = 24;
const MAX_STATES: usize = 10_000;
const TWO_SIDECAR_STATES: usize = 80;
const THREE_SIDECAR_STATES: usize = 183;
const SAW_CRASH: u8 = 1;
const SAW_FAILURE: u8 = 2;
const SAW_REPLAY: u8 = 4;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Phase {
    Idle,
    Sidecar { sidecar: usize, failed: bool },
    Block,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct DeletionState {
    phase: Phase,
    sidecars: Vec<bool>,
    block: bool,
    seen: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Action {
    Start,
    Delete,
    Fail,
    Crash,
}

struct DeletionModel {
    sidecars: usize,
}

impl Model for DeletionModel {
    type State = DeletionState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![DeletionState {
            phase: Phase::Idle,
            sidecars: vec![true; self.sidecars],
            block: true,
            seen: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        match state.phase {
            Phase::Idle if state.block => actions.push(Action::Start),
            Phase::Sidecar { .. } | Phase::Block => {
                actions.push(Action::Delete);
                actions.push(Action::Fail);
                actions.push(Action::Crash);
            }
            Phase::Idle => {}
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Start if matches!(state.phase, Phase::Idle) && state.block => {
                state.phase = Phase::Sidecar {
                    sidecar: 0,
                    failed: false,
                };
            }
            Action::Delete => match state.phase {
                Phase::Sidecar { sidecar, failed } => {
                    if !state.sidecars[sidecar] {
                        state.seen |= SAW_REPLAY;
                    }
                    state.sidecars[sidecar] = false;
                    state.phase = if sidecar + 1 == self.sidecars {
                        if failed { Phase::Idle } else { Phase::Block }
                    } else {
                        Phase::Sidecar {
                            sidecar: sidecar + 1,
                            failed,
                        }
                    };
                }
                Phase::Block => {
                    state.block = false;
                    state.phase = Phase::Idle;
                }
                Phase::Idle => return None,
            },
            Action::Fail => match state.phase {
                Phase::Sidecar { sidecar, .. } => {
                    state.seen |= SAW_FAILURE;
                    state.phase = if sidecar + 1 == self.sidecars {
                        Phase::Idle
                    } else {
                        Phase::Sidecar {
                            sidecar: sidecar + 1,
                            failed: true,
                        }
                    };
                }
                Phase::Block => {
                    state.seen |= SAW_FAILURE;
                    state.phase = Phase::Idle;
                }
                Phase::Idle => return None,
            },
            Action::Crash if !matches!(state.phase, Phase::Idle) => {
                state.seen |= SAW_CRASH;
                state.phase = Phase::Idle;
            }
            Action::Start | Action::Crash => return None,
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "a deleted block has no sidecars",
                |_, state: &DeletionState| {
                    state.block || state.sidecars.iter().all(|sidecar| !sidecar)
                },
            ),
            Property::sometimes("crash replay completes", |_, state: &DeletionState| {
                state.seen & SAW_CRASH != 0 && !state.block
            }),
            Property::sometimes("failure replay completes", |_, state: &DeletionState| {
                state.seen & SAW_FAILURE != 0 && !state.block
            }),
            Property::sometimes(
                "absent sidecars are replayed",
                |_, state: &DeletionState| state.seen & SAW_REPLAY != 0 && !state.block,
            ),
        ]
    }
}

fn check(sidecars: usize, expected_states: usize) {
    let checker = DeletionModel { sidecars }
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
fn two_sidecar_protocol_is_safe() {
    check(2, TWO_SIDECAR_STATES);
}

#[test]
fn three_sidecar_protocol_is_safe() {
    check(3, THREE_SIDECAR_STATES);
}

#[test]
fn failed_sidecar_does_not_stop_later_sidecars() {
    let model = DeletionModel { sidecars: 2 };
    let initial = model.init_states().pop().unwrap();
    let started = model.next_state(&initial, Action::Start).unwrap();
    let failed = model.next_state(&started, Action::Fail).unwrap();
    let complete = model.next_state(&failed, Action::Delete).unwrap();

    assert2::check!(matches!(
        failed.phase,
        Phase::Sidecar {
            sidecar: 1,
            failed: true
        }
    ));
    assert2::check!(!complete.sidecars[1]);
    assert2::check!(complete.block);
    assert2::check!(matches!(complete.phase, Phase::Idle));
}
