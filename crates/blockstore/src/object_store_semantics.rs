//! A startup probe for the object-store semantics Krabka depends on.
//!
//! Every role that reads or writes blocks calls
//! [`verify_object_store_semantics`] on its store before it serves or consumes
//! anything. A store that lacks a semantic then stops the role with an error
//! that names it, before any data is accepted. Without the probe, the same
//! store starts and fails later, on the first sweep or the first concurrent
//! writer, with an error far from its cause or with no error at all.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use object_store::{
    Error as ObjectStoreError, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode,
    PutPayload, PutResult, UpdateVersion, path::Path,
};
use tracing::instrument;
use url::Url;

mod conditional_update_requirement;
mod object_store_capabilities;
mod object_store_semantics_error;
mod verify_object_store_semantics;

pub use self::{
    conditional_update_requirement::ConditionalUpdateRequirement,
    object_store_capabilities::ObjectStoreCapabilities,
    object_store_semantics_error::ObjectStoreSemanticsError,
    verify_object_store_semantics::{OBJECT_STORE_PROBE_PREFIX, verify_object_store_semantics},
};
