//! The bounded breadth-first check that the storage protocol models share.

use std::{fmt::Debug, hash::Hash, time::Duration};

use stateright::{Checker, Model};

/// The search bounds of one model check, and the number of unique states the
/// search has to find inside them.
pub struct ModelBounds {
    pub max_depth: usize,
    pub max_states: usize,
    pub expected_states: usize,
}

/// Explores `model` breadth first within `bounds`, checks that the search
/// finished inside them and found exactly the expected states, and checks
/// every property of the model.
pub fn check_bounded_model<M>(model: M, bounds: &ModelBounds)
where
    M: Model + Send + Sync + 'static,
    M::State: Debug + Hash + Send + Sync + Clone + PartialEq + 'static,
    M::Action: Debug + Clone + PartialEq,
{
    let checker = model
        .checker()
        .target_max_depth(bounds.max_depth)
        .target_state_count(bounds.max_states)
        .timeout(Duration::from_secs(30))
        .spawn_bfs()
        .join();
    assert2::check!(checker.max_depth() < bounds.max_depth);
    assert2::check!(checker.state_count() < bounds.max_states);
    assert2::check!(checker.unique_state_count() == bounds.expected_states);
    checker.assert_properties();
}
