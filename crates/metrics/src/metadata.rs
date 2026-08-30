//! Per-tenant metric metadata index built from compacted WAL rows.

use std::collections::{BTreeMap, BTreeSet};

use crate::TenantCompactionRows;

// === split-modules: generated submodules ===
mod metadata_index;
mod metric_metadata;

pub use metadata_index::MetadataIndex;
pub use metric_metadata::MetricMetadata;
