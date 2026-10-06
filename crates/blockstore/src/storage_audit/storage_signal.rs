use krabka_domain_macros::EnumName;

use super::{Deserialize, Serialize};

/// The signal that owns an object in the store.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, EnumName,
)]
#[serde(rename_all = "snake_case")]
pub enum StorageSignal {
    #[name(value = "metrics")]
    Metrics,
    #[name(value = "logs")]
    Logs,
    #[name(value = "traces")]
    Traces,
    #[name(value = "profiles")]
    Profiles,
}

impl StorageSignal {
    /// Every signal, in report order.
    pub const ALL: [Self; 4] = [Self::Metrics, Self::Logs, Self::Traces, Self::Profiles];
}

impl std::str::FromStr for StorageSignal {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|signal| signal.as_str() == raw)
            .ok_or_else(|| {
                format!("unknown signal `{raw}`; expected metrics, logs, traces or profiles")
            })
    }
}
