use super::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtlpAttributeAction {
    IndexLabel,
    #[default]
    StructuredMetadata,
    Drop,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpAttributesConfig {
    #[serde(default)]
    pub action: OtlpAttributeAction,
    #[serde(default)]
    pub attributes: Vec<String>,
    #[serde(default)]
    pub regex: Option<String>,
}

impl OtlpAttributesConfig {
    pub(crate) fn matches(&self, name: &str) -> bool {
        self.attributes.iter().any(|attribute| attribute == name)
            || self
                .regex
                .as_ref()
                .is_some_and(|pattern| regex::Regex::new(pattern).is_ok_and(|re| re.is_match(name)))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpResourceAttributesConfig {
    #[serde(default)]
    pub ignore_defaults: bool,
    #[serde(default)]
    pub attributes_config: Vec<OtlpAttributesConfig>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpConfig {
    #[serde(default)]
    pub resource_attributes: OtlpResourceAttributesConfig,
    #[serde(default)]
    pub scope_attributes: Vec<OtlpAttributesConfig>,
    #[serde(default)]
    pub log_attributes: Vec<OtlpAttributesConfig>,
}
