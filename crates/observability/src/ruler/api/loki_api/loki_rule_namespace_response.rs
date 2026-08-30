use super::*;

pub(crate) fn loki_rule_namespace_response(
    namespaces: &LokiRuleNamespaces,
) -> BTreeMap<String, Vec<serde_yaml::Value>> {
    namespaces
        .iter()
        .map(|(namespace, groups)| (namespace.clone(), groups.values().cloned().collect()))
        .collect()
}
