use super::prelude::*;

/// `prometheus_alert_key_matches_rule` picks out the alerts belonging to
/// one rule that were NOT seen in this evaluation -- the ones that may need
/// retaining as resolved. All four conditions are and-ed, so each is broken
/// on its own against a key the other three accept.
///
/// The last is the negated one: a key still active this round is excluded,
/// which is what stops a firing alert being retained twice.
#[test]
pub(crate) fn a_retained_alert_key_belongs_to_its_rule_and_was_not_just_seen() {
    let key = |tenant: &str, alert: &str, query: &str| super::PrometheusAlertKey {
        tenant: tenant.to_string(),
        alert_name: alert.to_string(),
        query: query.to_string(),
        labels: Labels::default(),
    };
    let subject = key("tenant", "HighErrors", "up");
    let active = BTreeSet::new();
    let templates = Labels::default();
    let params = |active_keys| super::PrometheusRetainedAlertParams {
        tenant: "tenant",
        alert_name: "HighErrors",
        query: "up",
        evaluation_time: 0,
        hold_duration_ns: 0,
        keep_firing_for_ns: 0,
        active_keys,
        annotation_templates: &templates,
    };

    check!(super::prometheus_alert_key_matches_rule(
        &subject,
        &params(&active)
    ));

    // Each of the three identity fields, wrong on its own.
    check!(!super::prometheus_alert_key_matches_rule(
        &key("other", "HighErrors", "up"),
        &params(&active)
    ));
    check!(!super::prometheus_alert_key_matches_rule(
        &key("tenant", "Other", "up"),
        &params(&active)
    ));
    check!(!super::prometheus_alert_key_matches_rule(
        &key("tenant", "HighErrors", "down"),
        &params(&active)
    ));

    // And the negated one: a key seen this round is not retained.
    let mut seen = BTreeSet::new();
    seen.insert(subject.clone());
    check!(
        !super::prometheus_alert_key_matches_rule(&subject, &params(&seen)),
        "an alert still firing is not also retained"
    );
    // A different key being active does not exclude this one.
    let mut other_seen = BTreeSet::new();
    other_seen.insert(key("tenant", "HighErrors", "other"));
    check!(super::prometheus_alert_key_matches_rule(
        &subject,
        &params(&other_seen)
    ));
}

/// `matches_rule` filters the rules response by kind, by name, and by label
/// selector. The three are independent AND conditions, each inactive when
/// its filter is unset, so each is broken on its own against a rule the
/// other two accept.
///
/// The label selectors nest differently from the rest: SELECTORS are
/// or-ed and the matchers WITHIN a selector are and-ed, which is Loki's
/// `match[]` semantics. A single selector with a single matcher cannot
/// show that, so both nestings are exercised.
#[test]
pub(crate) fn a_rule_matches_only_when_every_active_filter_accepts_it() {
    use krabka_logql::{LabelMatcher, MatchOp, StreamQuery};

    let rule = serde_json::json!({"type": "alerting", "name": "HighErrors"});
    let source: serde_yaml::Value =
        serde_yaml::from_str("labels:\n  severity: page\n  team: infra\n")
            .expect("the source rule parses");
    let matcher = |name: &str, value: &str| LabelMatcher {
        name: name.to_string(),
        op: MatchOp::Equal,
        value: value.to_string(),
    };
    let selector = |matchers: Vec<LabelMatcher>| StreamQuery {
        matchers,
        pipeline: Vec::new(),
    };
    let filters =
        |kind, names: &[&str], selectors: Vec<StreamQuery>| super::PrometheusRulesFilters {
            rule_kind: kind,
            rule_names: names.iter().map(|name| (*name).to_string()).collect(),
            label_selectors: selectors,
            ..super::PrometheusRulesFilters::default()
        };

    // No filters at all accepts everything.
    check!(filters(None, &[], Vec::new()).matches_rule(&rule, &source));

    // Each filter alone, accepting and rejecting.
    check!(filters(Some("alerting"), &[], Vec::new()).matches_rule(&rule, &source));
    check!(!filters(Some("recording"), &[], Vec::new()).matches_rule(&rule, &source));
    check!(filters(None, &["HighErrors"], Vec::new()).matches_rule(&rule, &source));
    check!(!filters(None, &["Other"], Vec::new()).matches_rule(&rule, &source));
    check!(
        filters(None, &["Other", "HighErrors"], Vec::new()).matches_rule(&rule, &source),
        "any of the named rules"
    );

    // Matchers WITHIN a selector are and-ed: one wrong matcher rejects.
    check!(
        filters(None, &[], vec![selector(vec![matcher("severity", "page")])])
            .matches_rule(&rule, &source)
    );
    check!(
        !filters(
            None,
            &[],
            vec![selector(vec![
                matcher("severity", "page"),
                matcher("team", "billing"),
            ])],
        )
        .matches_rule(&rule, &source),
        "every matcher in a selector must match"
    );

    // SELECTORS are or-ed: one that fails does not reject if another
    // succeeds.
    check!(
        filters(
            None,
            &[],
            vec![
                selector(vec![matcher("team", "billing")]),
                selector(vec![matcher("team", "infra")]),
            ],
        )
        .matches_rule(&rule, &source),
        "any selector may match"
    );

    // All three active together, with one of them failing.
    check!(
        !filters(
            Some("alerting"),
            &["HighErrors"],
            vec![selector(vec![matcher("team", "billing")])],
        )
        .matches_rule(&rule, &source),
        "the label selector still rejects"
    );
}

/// `parse_patterns_params` requires query, start and end, defaults only the
/// step, and takes the LAST value of a repeated parameter. That last part
/// is the opposite of `parse_series_params`, which keeps the first -- the
/// two live in the same file and differ only by an `is_none()` guard, so
/// each is pinned with the contrast stated.
///
/// Each required parameter names ITSELF when missing, so a client sending
/// two of the three is told which one it forgot rather than a generic
/// failure.
#[test]
pub(crate) fn patterns_params_require_three_and_take_the_last_of_each() {
    let parse = |query: &str| super::parse_patterns_params(Some(query));

    let params = parse("query=up&start=100&end=200").expect("all three present");
    check!(params.query == "up");
    check!(params.start == 100);
    check!(params.end == 200);
    check!(
        params.step == 1_000_000_000,
        "the step defaults to a second"
    );

    // A repeated parameter keeps the LAST value.
    let params = parse("query=a&query=b&start=100&end=200").expect("parses");
    check!(params.query == "b", "the last query, unlike series params");
    let params = parse("query=up&start=100&start=300&end=200").expect("parses");
    check!(params.start == 300, "and the last start");

    // An explicit step overrides the default.
    let params = parse("query=up&start=100&end=200&step=5s").expect("parses");
    check!(params.step == 5_000_000_000);

    // Each required parameter names itself when absent.
    check!(matches!(
        parse("start=100&end=200"),
        Err(HttpQueryError::MissingQueryParameter("query"))
    ));
    check!(matches!(
        parse("query=up&end=200"),
        Err(HttpQueryError::MissingQueryParameter("start"))
    ));
    check!(matches!(
        parse("query=up&start=100"),
        Err(HttpQueryError::MissingQueryParameter("end"))
    ));
    check!(matches!(
        super::parse_patterns_params(None),
        Err(HttpQueryError::MissingQueryParameter("query"))
    ));

    // A malformed bound is refused rather than dropped.
    check!(parse("query=up&start=nonsense&end=200").is_err());
}

/// `parse_vector_matching_modifier` reads an `on(...)`/`ignoring(...)`
/// clause and returns BOTH the rendered modifier and the position just
/// past it. The position is what the caller resumes from, so an
/// off-by-one there leaves a stray bracket in the rest of the query --
/// each case checks the remainder, not just the modifier.
#[test]
pub(crate) fn a_vector_matching_modifier_reports_where_it_ended() {
    let parse = super::parse_vector_matching_modifier;
    let after = |query: &str, position: usize| {
        parse(query, position).map(|(modifier, end)| (modifier, query[end..].to_string()))
    };

    check!(
        after("on(job) foo", 0) == Some(("on (job)".to_string(), " foo".to_string())),
        "the remainder starts after the closing bracket"
    );
    check!(
        after("ignoring(pod) foo", 0) == Some(("ignoring (pod)".to_string(), " foo".to_string()))
    );
    check!(after("on(a,b) foo", 0) == Some(("on (a,b)".to_string(), " foo".to_string())));
    check!(
        after("on() foo", 0) == Some(("on ()".to_string(), " foo".to_string())),
        "an empty label list is still a modifier"
    );

    // Parsing from part-way in, which is how the caller uses it.
    check!(
        after("up on(job) foo", 3) == Some(("on (job)".to_string(), " foo".to_string())),
        "the position is an offset into the whole query"
    );

    // Not a modifier at this position.
    check!(parse("foo on(job)", 0).is_none());
    check!(parse("", 0).is_none());
    // The bracket must follow immediately: a space between is not this
    // spelling, and neither is an unclosed list.
    check!(parse("on (job)", 0).is_none());
    check!(parse("on(job", 0).is_none());
}

/// `format_logfmt_parser_flags` renders a parser's options back into the
/// query text. The leading space belongs to the FLAGS, not to the caller:
/// with no flags the string is empty rather than a lone space, which would
/// otherwise leave a trailing space in every query without options.
#[test]
pub(crate) fn logfmt_parser_flags_carry_their_own_leading_space() {
    use krabka_logql::{LogfmtExtraction, LogfmtParserConfig};

    // The flags are only accepted alongside an extraction, so every
    // config here names one.
    let flags = |strict, keep_empty| {
        let extraction = LogfmtExtraction::same("level").expect("a valid extraction");
        let config = LogfmtParserConfig::with_options(vec![extraction], strict, keep_empty)
            .expect("the options are valid");
        super::format_logfmt_parser_flags(&config)
    };

    check!(flags(false, false) == "", "no flags, no space");
    check!(flags(true, false) == " --strict");
    check!(flags(false, true) == " --keep-empty");
    check!(
        flags(true, true) == " --keep-empty --strict",
        "both, in a fixed order, sharing one leading space"
    );
}

/// `log_pattern_token` masks the variable part of a log token so lines that
/// differ only in their ids collapse to one pattern. A `key=value` token
/// keeps its KEY and masks only the value, because the key is what makes
/// two lines the same kind of line.
#[test]
pub(crate) fn a_log_pattern_token_masks_only_its_variable_part() {
    let token = super::log_pattern_token;

    // A bare token is masked whole, or kept whole.
    check!(token("connected") == "connected", "a word is not variable");
    check!(token("12345") == "<_>", "a number is");
    check!(token("1.5") == "<_>");

    // A key=value token keeps the key and masks the value.
    check!(token("user_id=12345") == "user_id=<_>");
    check!(
        token("status=ok") == "status=ok",
        "a non-variable value is kept"
    );
    check!(
        token("id=550e8400-e29b-41d4-a716-446655440000") == "id=<_>",
        "a uuid is variable"
    );

    // Half a pair is not a pair: an empty key or value leaves the token
    // alone rather than producing "=<_>" or "<_>=".
    check!(token("=12345") == "=12345");
    check!(token("user_id=") == "user_id=");
    check!(token("=") == "=");

    // Only the FIRST equals splits, so a value containing one is masked
    // whole rather than re-split.
    check!(
        token("q=a=12345") == "q=a=12345",
        "the value is not variable"
    );
    check!(token("") == "");
}

/// `could_be_scalar_vector_expression` is the cheap gate two of the query
/// parsers run before doing real work. It admits anything starting like a
/// number or a parenthesis, and among identifiers ONLY the three functions
/// that can produce a vector -- so `sum(...)` is turned away here and
/// parsed elsewhere.
#[test]
pub(crate) fn only_a_number_or_a_vector_function_could_be_a_scalar_vector_expression() {
    let could_be = super::could_be_scalar_vector_expression;

    // Numbers and the characters a numeric expression can open with.
    check!(could_be("1"));
    check!(could_be("1+1"));
    check!(could_be("+1"));
    check!(could_be("-1"));
    check!(could_be(".5"));
    check!(could_be("(1+1)"));
    check!(could_be("  1"), "leading whitespace is trimmed");

    // The three vector-producing functions, and nothing else.
    check!(could_be("vector(1)"));
    check!(could_be("label_replace(vector(1),\"a\",\"b\",\"c\",\"d\")"));
    check!(could_be("label_join(vector(1),\"a\",\"b\")"));
    check!(
        !could_be("sum(rate(x[5m]))"),
        "an aggregation is parsed elsewhere"
    );
    check!(!could_be("up"));

    // The identifier must match WHOLE: a longer name starting with one of
    // the three is not one of them.
    check!(!could_be("vectorise(1)"));
    check!(!could_be("vector_total"));

    // Nothing, and things that start with neither.
    check!(!could_be(""));
    check!(!could_be("   "));
    check!(
        !could_be("{app=\"a\"}"),
        "a matcher is not a scalar expression"
    );
    check!(!could_be("\"quoted\""));
}

/// `insert_descriptor_labels` copies a block's series labels from one index
/// to another, and REFUSES when the source cannot supply them. A missing
/// series is a corrupt index rather than an empty block, so carrying on
/// would write a manifest whose blocks reference series nothing can name.
#[test]
pub(crate) fn copying_descriptor_labels_refuses_a_series_the_source_cannot_name() {
    use krabka_blockstore::{BlockDescriptor, BlockKey, LabelIndex, TimeRange};

    let mut source = LabelIndex::default();
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    let known = source.insert_series("tenant", labels.clone());
    let mut other = Labels::default();
    other.insert("app".to_string(), "web".to_string());
    let also_known = source.insert_series("tenant", other.clone());

    let descriptor = |fingerprints: &[_]| {
        BlockDescriptor::new(
            BlockKey::new("tenant", 0, 0, 1, TimeRange::new(0, 10).expect("a range")),
            fingerprints.iter().copied().collect(),
        )
    };

    // Both series are known, so both are copied.
    let mut target = LabelIndex::default();
    super::insert_descriptor_labels(
        &mut target,
        &source,
        "tenant",
        &descriptor(&[known, also_known]),
    )
    .expect("both series are known");
    check!(target.labels_for("tenant", known) == Some(&labels));
    check!(target.labels_for("tenant", also_known) == Some(&other));

    // A fingerprint the source has never seen is refused, and the error
    // names which one so the corruption can be found.
    let mut target = LabelIndex::default();
    let stranger = LabelIndex::default().insert_series("tenant", {
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "stranger".to_string());
        labels
    });
    check!(matches!(
        super::insert_descriptor_labels(&mut target, &source, "tenant", &descriptor(&[stranger])),
        Err(CompactorRunError::MissingSeriesLabels { .. })
    ));

    // The labels belong to a TENANT, so the right fingerprint under the
    // wrong tenant is just as unknown.
    let mut target = LabelIndex::default();
    check!(
        super::insert_descriptor_labels(&mut target, &source, "other", &descriptor(&[known]))
            .is_err(),
        "a fingerprint is not global"
    );

    // A descriptor with no series copies nothing and succeeds.
    let mut target = LabelIndex::default();
    super::insert_descriptor_labels(&mut target, &source, "tenant", &descriptor(&[]))
        .expect("an empty descriptor is not an error");
    check!(target.labels_for("tenant", known).is_none());
}
