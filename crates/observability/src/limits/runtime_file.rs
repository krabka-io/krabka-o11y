use super::{Deserialize, HashMap, PartialLimits};

/// The shape of the file `--logs-limits-overrides-config` names.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeFile {
    #[serde(default)]
    pub(crate) defaults: PartialLimits,
    #[serde(default)]
    pub(crate) overrides: HashMap<String, PartialLimits>,
}
