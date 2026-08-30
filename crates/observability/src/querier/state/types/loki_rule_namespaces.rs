use super::{BTreeMap, LokiRuleGroupsByName};

pub(crate) type LokiRuleNamespaces = BTreeMap<String, LokiRuleGroupsByName>;
