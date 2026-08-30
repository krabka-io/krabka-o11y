//! Custom `DataFusion` operators used to model `PromQL` vectors.
//!
//! These nodes carry window widths: a step, a lookback delta, a range, and a
//! grid interval. The widths are extents, but they stay raw `i64` milliseconds
//! here instead of [`Time`](krabka_units::Time) quantities.
//! `UserDefinedLogicalNodeCore` needs `Eq` and `Hash` so that the `DataFusion`
//! planner can key on nodes and deduplicate them, and a quantity stores `f64`,
//! so a quantity can be neither. The paired `*Exec` nodes hold the same raw
//! integers, which also keeps the per-row timestamp arithmetic in integer
//! space. The seam is the planner in [`crate::planner`], which converts a `Time`
//! into milliseconds exactly once as it builds the node.

pub mod instant_manipulate;
pub mod normalize;
pub mod planner;
pub mod range_manipulate;
pub mod series_divide;

mod is_stale_nan;
mod stale_nan_bits;

pub(crate) use is_stale_nan::is_stale_nan;
pub(crate) use stale_nan_bits::STALE_NAN_BITS;
