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

mod default_ms;
mod end_ms;
mod external_partition;
mod ingest_bytes;
mod ingest_items;
mod local_partition;
mod max_value;
mod min_value;
mod now_ms;
mod start_ms;

pub use default_ms::DefaultMs;
pub use end_ms::EndMs;
pub use external_partition::ExternalPartition;
pub use ingest_bytes::IngestBytes;
pub use ingest_items::IngestItems;
pub use local_partition::LocalPartition;
pub use max_value::MaxValue;
pub use min_value::MinValue;
pub use now_ms::NowMs;
pub use start_ms::StartMs;
