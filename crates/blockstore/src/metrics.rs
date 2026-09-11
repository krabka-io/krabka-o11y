//! Object-store instruments for the block store.
//!
//! Every one of the four signals reads and writes its blocks through this
//! crate, so an object store that degrades degrades all of them at once. The
//! symptom an operator sees without these instruments is a query that returns
//! stale data, which names neither the store nor the operation.
//!
//! [`ObjectStoreMetrics`] holds the instruments and [`MeteredObjectStore`]
//! records them. The decorator is the only choke point this crate has: the
//! `Arc<dyn ObjectStore>` is threaded through about thirty modules that call
//! `put_opts`, `get_opts`, `list` and `delete_stream` directly, and
//! `DataFusion` reaches the store on its own to scan Parquet. A service wraps
//! the store once, where it builds it, and every one of those paths is then
//! counted.
//!
//! # Names
//!
//! The instruments register into an `objstore` sub-registry of the service's
//! own registry, so the traces service exports
//! `krabka_traces_objstore_operation_duration_seconds`. That matches
//! `thanos_objstore_bucket_operation_duration_seconds`, which Loki, Mimir and
//! Tempo all export from the same shared bucket client, and it keeps one
//! series per service rather than one shared series for the whole stack.
//!
//! # Cardinality
//!
//! The only label is `operation`, and [`ObjectStoreOperation`] is a closed
//! enum, so the label set is bounded by the seven operations the
//! [`ObjectStore`](object_store::ObjectStore) trait requires. There is no
//! `tenant` label and no `path` label. Both are unbounded, both sit on the
//! hottest path in the crate, and a block key carries a tenant, a partition
//! and an offset range, so a `path` label would make one series per block.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use async_trait::async_trait;
use futures::{Stream, stream::BoxStream};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt, TimeExt},
};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};

#[cfg(test)]
mod tests;

mod metered_object_store;
mod metered_stream;
mod object_store_metrics;
mod object_store_operation;
mod object_store_operation_label;

use self::metered_stream::MeteredStream;
pub use self::{
    metered_object_store::MeteredObjectStore, object_store_metrics::ObjectStoreMetrics,
    object_store_operation::ObjectStoreOperation,
    object_store_operation_label::ObjectStoreOperationLabel,
};
