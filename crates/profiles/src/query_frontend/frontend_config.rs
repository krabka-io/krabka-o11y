use super::*;

/// Not `Eq`: [`Time`] stores `f64`. Nothing keys a map on this config.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrontendConfig {
    /// Width of each shard that the query range splits into.
    pub shard_width: Time,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            shard_width: minutes(15),
        }
    }
}
