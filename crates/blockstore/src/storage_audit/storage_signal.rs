use super::{Deserialize, Serialize, fmt};

/// The signal that owns an object in the store.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageSignal {
    Metrics,
    Logs,
    Traces,
    Profiles,
}

impl StorageSignal {
    /// Every signal, in report order.
    pub const ALL: [Self; 4] = [Self::Metrics, Self::Logs, Self::Traces, Self::Profiles];

    /// The name the report and the command line use.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Logs => "logs",
            Self::Traces => "traces",
            Self::Profiles => "profiles",
        }
    }
}

impl fmt::Display for StorageSignal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
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
