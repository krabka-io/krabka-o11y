use super::*;

/// A negative range offset MUST render with a leading `-` sign, and a positive
/// offset MUST NOT. This pins the `offset_ns.0 < 0` sign branch in
/// `format_metric_range_selector`. A replacement of `<` with `==` would
/// drop the sign and emit a positive offset for a query that asked to look
/// *forward* in time. That `==` is never true here, because the outer guard
/// handles the `== 0` case.
#[test]
pub(crate) fn format_metric_range_selector_signs_negative_offset() {
    let negative = parse_metric_query("count_over_time({app=\"x\"}[5m] offset -3m)").unwrap();
    let positive = parse_metric_query("count_over_time({app=\"x\"}[5m] offset 3m)").unwrap();

    let negative_selector =
        format_metric_range_selector(&negative).expect("negative offset selector");
    let positive_selector =
        format_metric_range_selector(&positive).expect("positive offset selector");

    // The negative offset carries the sign; the positive one does not.
    check!(negative_selector.contains(" offset -"));
    check!(!positive_selector.contains(" offset -"));
    // The two differ ONLY by the sign character.
    check!(negative_selector == positive_selector.replace(" offset ", " offset -"));
}
