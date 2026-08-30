use super::{Deserialize, HashMap, PartialLimits};

#[derive(Deserialize)]
pub(crate) struct RuntimeFile {
    #[serde(default)]
    pub(crate) overrides: HashMap<String, PartialLimits>,
}
