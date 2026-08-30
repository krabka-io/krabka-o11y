use super::{Deserialize, PartialLimits, HashMap};

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RuntimeFile {
    #[serde(default)]
    pub(crate) defaults: PartialLimits,
    #[serde(default)]
    pub(crate) overrides: HashMap<String, PartialLimits>,
}
