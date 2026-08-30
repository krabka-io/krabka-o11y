//! Domain newtypes for the `LogQL` front-end.
//!
//! These types wrap the bare `i64`, `u64`, and `String` values that recur in
//! the query AST. Two values of the same type but with different meanings can
//! thus not be transposed at a call site and still compile. Such pairs are a
//! range duration and a query offset, a quantile numerator and its denominator,
//! and an extraction destination and its source.

use derive_more::{Display, From, Into};

// === split-modules: generated submodules ===
mod destination_label;
mod duration_nanos;
mod json_expression_path;
mod offset_nanos;
mod quantile_denominator;
mod quantile_numerator;
mod source_label;

pub use destination_label::DestinationLabel;
pub use duration_nanos::DurationNanos;
pub use json_expression_path::JsonExpressionPath;
pub use offset_nanos::OffsetNanos;
pub use quantile_denominator::QuantileDenominator;
pub use quantile_numerator::QuantileNumerator;
pub use source_label::SourceLabel;
