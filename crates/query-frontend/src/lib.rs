//! Signal-agnostic planning, fan-out, caching, and merge orchestration.

mod admission;
mod cache;
mod execute;

pub use admission::{
    AdmissionController, AdmissionError, AdmissionLimits, AdmissionLimitsOverride, AdmissionPermit,
};
pub use cache::{
    CacheKey, CacheMetrics, CacheMetricsSnapshot, CachePolicy, Clock, InMemoryCache,
    ObjectStoreCache, ObjectStoreCacheError, QueryCache, SystemClock,
};
pub use execute::{
    ExecutionOptions, PlannedQuery, QueryFrontend, QueryFrontendAdapter, QueryFrontendError,
};

#[cfg(test)]
mod tests;
