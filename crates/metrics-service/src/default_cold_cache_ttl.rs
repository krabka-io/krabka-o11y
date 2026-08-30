use super::{Time, secs};

/// Time that the service serves a cached cold-block store snapshot before it
/// lists the manifests again from the object store.
pub const DEFAULT_COLD_CACHE_TTL: Time = secs(30);
