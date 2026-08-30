use super::*;

/// Every rules filter that takes a value ignores an empty one. Without
/// that guard `time=`, `group_limit=` and `match=` are handed the empty
/// string to parse and the whole request fails, while `rule_group=`,
/// `file=` and `group_next_token=` quietly filter on "" and match nothing.
/// A query naming all of them with no values is indistinguishable from no
/// query at all.
#[test]
pub(crate) fn empty_prometheus_rules_filter_values_are_ignored() {
    let filters = super::super::prelude::PrometheusRulesFilters::parse(Some(
        "time=&rule_name=&rule_group=&file=&group_limit=&group_next_token=&match=",
    ))
    .expect("empty values are ignored, not rejected");

    check!(filters == super::super::prelude::PrometheusRulesFilters::default());
}
