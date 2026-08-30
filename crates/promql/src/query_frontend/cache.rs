use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use krabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};

use super::{FrontendRangeQuery, QueryShard};
use crate::{PromqlError, QueryResult};

// === split-modules: generated submodules ===
mod append_hex_component;
mod cache_store_error;
mod clock;
mod entry_is_expired;
mod normalize_cache_prefix;
mod object_store_query_frontend_cache;
mod query_frontend_cache;
mod range_cache_key;
mod range_cache_key_object_name;
mod range_query_cache;
mod stored_range_result;
mod system_clock;

use append_hex_component::append_hex_component;
use cache_store_error::cache_store_error;
pub use clock::Clock;
use entry_is_expired::entry_is_expired;
use normalize_cache_prefix::normalize_cache_prefix;
pub use object_store_query_frontend_cache::ObjectStoreQueryFrontendCache;
pub use query_frontend_cache::QueryFrontendCache;
pub (super) use range_cache_key::RangeCacheKey;
use range_cache_key_object_name::range_cache_key_object_name;
pub use range_query_cache::RangeQueryCache;
use stored_range_result::StoredRangeResult;
pub use system_clock::SystemClock;
