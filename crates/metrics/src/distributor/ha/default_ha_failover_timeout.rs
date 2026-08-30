use super::{Time, secs};

/// Default elected-replica lease timeout before another replica may take over.
pub const DEFAULT_HA_FAILOVER_TIMEOUT: Time = secs(30);
