//! Signal-agnostic planning, fan-out, caching, and merge orchestration.

mod cache;
mod execute;

pub use cache::{
    CacheKey, Clock, InMemoryCache, ObjectStoreCache, ObjectStoreCacheError, QueryCache,
    SystemClock,
};
pub use execute::{
    ExecutionOptions, PlannedQuery, QueryFrontend, QueryFrontendAdapter, QueryFrontendError,
};

#[cfg(test)]
mod tests;
