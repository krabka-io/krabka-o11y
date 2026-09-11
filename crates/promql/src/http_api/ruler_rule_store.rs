use super::{BTreeMap, TenantId};

pub(crate) type RulerRuleStore =
    BTreeMap<TenantId, BTreeMap<String, BTreeMap<String, serde_yaml::Value>>>;
