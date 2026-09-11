//! Compaction instruments, shared by the four signals.
//!
//! Each signal counts the blocks its compactor produced and nothing else:
//! `blocks_compacted`, `blocks_flushed`, `blocks_built`, `blocks_written`.
//! Every one of those is a success counter with no failure beside it, so a
//! compactor whose passes all fail has the same exported state as a compactor
//! with nothing to do. Two of the four signals swallow a failed pass with a
//! `warn!` and go back to sleep, which makes the log the only record that the
//! pass ran at all.
//!
//! [`CompactionMetrics`] adds the three readings that separate those states:
//! how many passes ran, how many failed, and how long a pass takes.
//!
//! # Names
//!
//! The instruments register into a `compaction` sub-registry of the service's
//! own registry, so the traces service exports
//! `krabka_traces_compaction_duration_seconds` beside Tempo's
//! `tempodb_compaction_duration_seconds`, and
//! `krabka_traces_compaction_runs_total{status="error"}` beside Mimir's
//! `cortex_compactor_runs_failed_total`.
//!
//! # Cardinality
//!
//! The only label is `status`, and it takes two values. There is no `tenant`
//! label: a compaction pass covers every tenant whose blocks the planner
//! chose, so the pass has no single tenant to attribute, and the per-tenant
//! output is already visible as blocks in the index.

use krabka_units::{Time, convert::TimeExt};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};

#[cfg(test)]
mod tests;

mod compaction_metrics_bundle;
mod compaction_status_label;

pub use self::{
    compaction_metrics_bundle::CompactionMetrics, compaction_status_label::CompactionStatusLabel,
};
