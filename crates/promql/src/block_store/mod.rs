//! `MetricStore` adapter backed by `krabka-blockstore`.

use krabka_blockstore::{BlockMeta, BlockStore};
use krabka_metrics::{CompactionIndexManifest, MetricBlockKind};

mod store_impl;

#[cfg(test)]
mod tests;

mod apply_manifest_to_blockstore;
mod metric_block_store;

use apply_manifest_to_blockstore::apply_manifest_to_blockstore;
pub use metric_block_store::MetricBlockStore;
