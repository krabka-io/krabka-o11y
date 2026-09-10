//! Grid-driven leaf results, memoized across a range query's step loop.
//!
//! The per-step range driver plans and executes a `DataFusion` plan once per
//! grid step. Nearly all of that cost is fixed: it does not scale with the
//! window's data, so at low cardinality it is the whole cost of the query. With
//! the engine's cap of 11,000 resolution points it is up to 11,000 planning and
//! execution round trips for one query.
//!
//! The operator leaves do not need it. [`InstantManipulate`] and
//! [`RangeManipulate`] have always taken a `(start, end, step)` grid, and the
//! per-step planner simply drove them with a one-point grid. So the first time
//! the step loop reaches a leaf, this module plans and executes that leaf over
//! the query's **whole** grid, keeps the per-step results here, and answers the
//! rest of the loop's steps from them. One plan and one execution replace one
//! per step, for every expression shape the leaves appear in — a bare selector,
//! a rate, or the inner of an aggregation — because the memo sits at the leaf
//! rather than at the top of the tree.
//!
//! # Why the results, and not the plan, are what is kept
//!
//! A leaf's per-step result is a function of the leaf and the instant alone, so
//! serving a step from this cache is the same value the step's own plan would
//! have produced. That keeps the range driver, the recursive planner, and every
//! assembler untouched: a memoized leaf simply arrives as a
//! [`PlannedInstant::Precomputed`](super::planned::PlannedInstant::Precomputed).
//!
//! # Scope, keys, and what is deliberately not cached
//!
//! [`RANGE_STEP_VECTORS`] is scoped by the range driver, exactly as
//! `RANGE_SCAN_CACHE` is, so a nested range evaluation (a subquery's own grid)
//! shadows it with a fresh cache and restores the outer one on exit. A leaf's
//! key is its rendered `PromQL` text plus the fold applied over it, so two
//! textually identical leaves share one entry — which is sound, because they
//! evaluate identically — and no two different leaves ever share one.
//!
//! Every lookup is still checked against the grid: a request for an instant that
//! is not on it, such as a subquery's sub-step, misses and falls through to the
//! per-step plan. A selector carrying an `@` modifier is never memoized, because
//! its evaluation instant is pinned rather than driven by the grid. And a leaf
//! carrying a scalar parameter — `quantile_over_time`'s `phi`, which is resolved
//! per step and so may differ between them — is given up the moment that
//! parameter changes, rather than planned afresh over the whole grid at every
//! step.
//!
//! [`InstantManipulate`]: crate::extension::instant_manipulate::InstantManipulate
//! [`RangeManipulate`]: crate::extension::range_manipulate::RangeManipulate

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use krabka_blockstore::{Labels, SeriesFingerprint};

use crate::{
    planner::StepGrid,
    result::{InstantSample, SampleValue},
};

tokio::task_local! {
    /// Active only for the dynamic extent of the step loop in
    /// `PromqlEngine::eval_range_via_planner`. Nested range evaluations
    /// (subqueries) shadow it with their own cache, keyed on their own grid, and
    /// restore the outer cache on exit.
    pub(super) static RANGE_STEP_VECTORS: StepVectorCache;
}

mod grid_point;
mod grid_vectors;
mod leaf_lookup;
mod leaf_memo;
mod step_vector_cache;
mod step_vector_cache_inner;

pub(super) use grid_point::GridPoint;
pub(super) use grid_vectors::GridVectors;
pub(super) use leaf_lookup::LeafLookup;
pub(super) use leaf_memo::LeafMemo;
pub(super) use step_vector_cache::StepVectorCache;
pub(super) use step_vector_cache_inner::StepVectorCacheInner;
