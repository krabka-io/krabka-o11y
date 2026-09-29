//! Object-store request and byte counts, per phase and per path.

use std::sync::Arc;

use krabka_blockstore::{MeteredObjectStore, ObjectStoreMetrics, ObjectStoreOperation};
use object_store::{ObjectStore, prefix::PrefixStore};
use serde_json::{Map, Value, json};

use super::{Stores, latency::ratio};

/// The counters behind one phase's three [`Stores`].
#[derive(Clone)]
pub struct StoreMeters {
    pub write: ObjectStoreMetrics,
    pub read: ObjectStoreMetrics,
    pub maintenance: ObjectStoreMetrics,
}

impl StoreMeters {
    /// Wraps `backing` under `prefix` three times, each with its own counters.
    pub fn wrap(backing: &Arc<dyn ObjectStore>, prefix: &str) -> (Stores, Self) {
        let meters = Self {
            write: ObjectStoreMetrics::unregistered(),
            read: ObjectStoreMetrics::unregistered(),
            maintenance: ObjectStoreMetrics::unregistered(),
        };
        let scoped = || -> Arc<dyn ObjectStore> {
            Arc::new(PrefixStore::new(Arc::clone(backing), prefix.to_string()))
        };
        let stores = Stores {
            write: MeteredObjectStore::wrap(scoped(), meters.write.clone()),
            read: MeteredObjectStore::wrap(scoped(), meters.read.clone()),
            maintenance: MeteredObjectStore::wrap(scoped(), meters.maintenance.clone()),
        };
        (stores, meters)
    }

    pub fn snapshot(&self) -> MetersSnapshot {
        MetersSnapshot {
            write: Counts::take(&self.write),
            read: Counts::take(&self.read),
            maintenance: Counts::take(&self.maintenance),
        }
    }
}

/// The three paths' counters at one instant.
#[derive(Clone, Copy, Debug, Default)]
pub struct MetersSnapshot {
    pub write: Counts,
    pub read: Counts,
    pub maintenance: Counts,
}

impl MetersSnapshot {
    /// The report form of `self - earlier`. Each path's per-operation ratio
    /// divides by the operations that path served in the same window.
    pub fn report_since(
        &self,
        earlier: &Self,
        writes: u64,
        reads: u64,
        maintenance_passes: u64,
    ) -> Value {
        json!({
            "write": self.write.since(&earlier.write).report(writes),
            "read": self.read.since(&earlier.read).report(reads),
            "maintenance": self.maintenance.since(&earlier.maintenance).report(maintenance_passes),
        })
    }
}

const OPERATIONS: usize = ObjectStoreOperation::all().len();

/// One path's requests, bytes and failures, per operation.
#[derive(Clone, Copy, Debug, Default)]
pub struct Counts {
    requests: [u64; OPERATIONS],
    bytes: [u64; OPERATIONS],
    failures: [u64; OPERATIONS],
}

impl Counts {
    fn take(metrics: &ObjectStoreMetrics) -> Self {
        let mut counts = Self::default();
        for (slot, operation) in ObjectStoreOperation::all().into_iter().enumerate() {
            counts.requests[slot] = metrics.operations(operation);
            counts.bytes[slot] = metrics.transferred_bytes(operation);
            counts.failures[slot] = metrics.failures(operation);
        }
        counts
    }

    fn since(&self, earlier: &Self) -> Self {
        let mut delta = Self::default();
        for slot in 0..OPERATIONS {
            delta.requests[slot] = self.requests[slot].saturating_sub(earlier.requests[slot]);
            delta.bytes[slot] = self.bytes[slot].saturating_sub(earlier.bytes[slot]);
            delta.failures[slot] = self.failures[slot].saturating_sub(earlier.failures[slot]);
        }
        delta
    }

    /// `write_block` wraps the puts it makes, so counting it as well would
    /// count those puts twice. It is reported, and left out of the totals.
    fn report(&self, ops: u64) -> Value {
        let mut requests = Map::new();
        let mut bytes = Map::new();
        let mut total_requests = 0;
        let mut total_bytes = 0;
        for (slot, operation) in ObjectStoreOperation::all().into_iter().enumerate() {
            requests.insert(operation.as_str().into(), self.requests[slot].into());
            bytes.insert(operation.as_str().into(), self.bytes[slot].into());
            if operation != ObjectStoreOperation::WriteBlock {
                total_requests += self.requests[slot];
                total_bytes += self.bytes[slot];
            }
        }
        json!({
            "ops": ops,
            "requests": requests,
            "bytes": bytes,
            "failures": self.failures.iter().sum::<u64>(),
            "total_requests": total_requests,
            "total_bytes": total_bytes,
            "requests_per_op": ratio(total_requests, ops),
            "bytes_per_op": ratio(total_bytes, ops),
        })
    }
}
