use super::{Deserialize, HashMap, PartialLimits};

// `deny_unknown_fields` rejects typo'd / unsupported keys at load instead of
// silently ignoring them (a footgun where an operator's intended limit never
// takes effect).
//
// `defaults` names the limits every tenant starts from, and `overrides` names
// the per-tenant departures from them. Both are partial, so a file that sets
// neither leaves the compiled-in Pyroscope defaults in place.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeFile {
    #[serde(default)]
    pub(crate) defaults: PartialLimits,
    #[serde(default)]
    pub(crate) overrides: HashMap<String, PartialLimits>,
}
