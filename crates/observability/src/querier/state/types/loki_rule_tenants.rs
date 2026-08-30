use super::{BTreeMap, LokiRuleNamespaces};

pub(crate) type LokiRuleTenants = BTreeMap<String, LokiRuleNamespaces>;
