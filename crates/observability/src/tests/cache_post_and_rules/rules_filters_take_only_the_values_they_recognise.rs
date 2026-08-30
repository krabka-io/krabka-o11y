use super::*;

/// The rules filters read a `Prometheus`-shaped query. Each recognised key
/// is guarded on its value, so a key carrying something unexpected leaves
/// the filter unset rather than setting it to a default.
#[test]
pub(crate) fn rules_filters_take_only_the_values_they_recognise() {
    use super::super::prelude::PrometheusRulesFilters as Filters;
    let parse = |q: &str| Filters::parse(Some(q)).expect("valid query");

    // `type` maps two spellings and rejects the rest.
    check!(parse("type=alert").rule_kind == Some("alerting"));
    check!(parse("type=record").rule_kind == Some("recording"));
    check!(
        parse("type=other").rule_kind == None,
        "an unknown type sets nothing"
    );
    check!(parse("type=").rule_kind == None);
    check!(
        parse("type=alerting").rule_kind == None,
        "the output spelling is not the input"
    );

    // `exclude_alerts` is only true for the exact string.
    check!(parse("exclude_alerts=true").exclude_alerts);
    check!(!parse("exclude_alerts=false").exclude_alerts);
    check!(
        !parse("exclude_alerts=1").exclude_alerts,
        "only `true` counts"
    );
    check!(
        !parse("exclude_alerts=TRUE").exclude_alerts,
        "case-sensitively"
    );
    check!(!Filters::parse(None).expect("no query").exclude_alerts);

    // The repeated keys accept both spellings and collect rather than
    // replace, and an empty value is skipped rather than collected.
    let names = parse("rule_name=a&rule_name[]=b&rule_name=").rule_names;
    check!(names.len() == 2, "got {names:?}");
    check!(names.contains("a") && names.contains("b"));

    let groups = parse("rule_group=g1&rule_group[]=g2").rule_groups;
    check!(groups.len() == 2);

    // No query at all is a default set of filters, not an error.
    let empty = Filters::parse(None).expect("no query");
    check!(empty.rule_kind == None);
    check!(empty.rule_names.is_empty());
}
