use super::*;

pub(crate) fn prometheus_rule_group_page_token(namespace: &str, group_name: &str) -> String {
    URL_SAFE_NO_PAD.encode(format!("{namespace}\n{group_name}"))
}
