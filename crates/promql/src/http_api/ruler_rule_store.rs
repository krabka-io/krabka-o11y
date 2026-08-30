use super::BTreeMap;

pub(crate) type RulerRuleStore =
    BTreeMap<String, BTreeMap<String, BTreeMap<String, serde_yaml::Value>>>;
