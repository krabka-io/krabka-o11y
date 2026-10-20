//! Newtypes for same-typed profiles domain values that a call site could
//! transpose and still compile.
//!
//! Each wrapper here guards a specific swap-shaped signature: two or more
//! adjacent parameters of the same primitive type whose meanings differ. In such
//! a signature, the wrong order is a silent bug. None of these values are
//! serialised, because they are query-path and metric arguments plus one
//! in-memory partition-map key, so they need no `#[serde(transparent)]`. The few
//! sites that need arithmetic use the inner value through `.0`, so this module
//! does not derive `Add` or `Sub`.

use derive_more::{Display, From, Into};

/// The fallback value a render-time parameter takes when absent, in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct DefaultMs(pub i64);

/// A query-window end bound, in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct EndMs(pub i64);

/// A partition key in the composite cold-read address space. Symbol resolution routes on this key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct ExternalPartition(pub u64);

/// A partition key within a single block's own symbol DB, scoped to that block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct LocalPartition(pub u64);

/// The upper edge of a heatmap's value axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct MaxValue(pub i64);

/// The lower edge of a heatmap's value axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct MinValue(pub i64);

/// The current wall-clock instant a relative render time resolves against, in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct NowMs(pub i64);

/// A query-window start bound, in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct StartMs(pub i64);
