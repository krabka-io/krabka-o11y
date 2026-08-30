use super::{Deserialize, HashMap, PartialLimits};

// `deny_unknown_fields` rejects typo'd / unsupported keys at load instead of
// silently ignoring them (a footgun where an operator's intended limit never
// takes effect).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeFile {
    #[serde(default)]
    pub(crate) overrides: HashMap<String, PartialLimits>,
}
