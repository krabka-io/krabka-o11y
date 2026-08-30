use super::*;

#[test]
pub(crate) fn prometheus_rules_filters_parse_all_supported_axes() {
    let filters = PrometheusRulesFilters::parse(Some(
            "type=alert&exclude_alerts=true&time=10&rule_name=HighError&rule_group=api&file=rules.yaml&group_limit=2&group_next_token=next&match=%7Bapp%3D%22api%22%7D",
        ))
        .unwrap();
    assert_eq!(filters.rule_kind, Some("alerting"));
    check!(filters.exclude_alerts);
    check!(filters.evaluation_time.is_some());
    check!(filters.rule_names.contains("HighError"));
    check!(filters.rule_groups.contains("api"));
    check!(filters.files.contains("rules.yaml"));
    assert_eq!(filters.group_limit, Some(2));
    assert_eq!(filters.group_next_token.as_deref(), Some("next"));
    assert_eq!(filters.label_selectors.len(), 1);
    assert!(filters.has_rule_filter());

    let recording = PrometheusRulesFilters::parse(Some("type=record")).unwrap();
    assert_eq!(recording.rule_kind, Some("recording"));
    assert!(PrometheusRulesFilters::parse(Some("group_next_token=next")).is_err());
    assert!(
        !PrometheusRulesFilters::parse(Some(""))
            .unwrap()
            .has_rule_filter()
    );
}
