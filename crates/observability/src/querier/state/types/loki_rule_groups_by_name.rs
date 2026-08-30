use super::BTreeMap;

pub(crate) type LokiRuleGroupsByName = BTreeMap<String, serde_yaml::Value>;
