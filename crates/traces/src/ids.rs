//! Newtypes for the traces domain values that are otherwise bare primitives.
//!
//! Several helpers in this crate thread two or more same-typed `i64`s that mean
//! different things. Examples are a start/end nanosecond pair, a Jaeger
//! trace-id `(high, low)` word pair, and the
//! `(min_offset, max_offset, window_start_ns)` triple that composes a
//! deterministic object key. Bare `i64`s let a caller transpose them and still
//! compile, which is the textbook swap bug. These wrappers make the compiler
//! reject a mixed-up call site.
//!
//! All values here are pure in-memory scalars threaded through function
//! signatures. None of them are serialised, so none need
//! `#[serde(transparent)]`. The serialised WAL and Arrow span fields keep their
//! raw `i64` representation, because the swap surface is the *call site*, not
//! the stored record.
//!
//! Arithmetic runs on the inner `i64` through `.0` at the point of use, because
//! the operations cross newtype boundaries. `end - start` and the `min(...)`
//! and `max(...)` of an offset pair are two such operations. `Add` and `Sub`
//! are therefore not derived, on purpose.

use derive_more::{Display, From, Into};

mod max_offset;
mod min_offset;
mod trace_id_high;
mod trace_id_low;
mod unix_nano;
mod window_start_ns;

pub use max_offset::MaxOffset;
pub use min_offset::MinOffset;
pub use trace_id_high::TraceIdHigh;
pub use trace_id_low::TraceIdLow;
pub use unix_nano::UnixNano;
pub use window_start_ns::WindowStartNs;
