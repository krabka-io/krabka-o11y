use super::*;

/// The detected-fields parser carries the same first-wins contract.
#[test]
pub(crate) fn a_repeated_detected_fields_parameter_keeps_the_first_value() {
    let parse =
        |q: &str| super::super::prelude::parse_detected_fields_params(Some(q)).expect("valid query");

    check!(parse("query=a&query=b").query == "a");
    check!(parse("query=a&limit=5&limit=9").limit == 5);
    check!(parse("query=a&start=100&start=200").start == 100);
    check!(parse("query=a&end=500&end=900").end == 500);
    check!(parse("query=a&line_limit=7&line_limit=11").line_limit == 7);

    // `field_limit` is an alias for `limit`, guarded on the same field, so
    // first-wins spans the pair rather than each name separately.
    check!(
        parse("query=a&field_limit=9").limit == 9,
        "the alias sets limit"
    );
    check!(
        parse("query=a&limit=5&field_limit=9").limit == 5,
        "limit first"
    );
    check!(
        parse("query=a&field_limit=9&limit=5").limit == 9,
        "alias first"
    );

    // Defaults apply when absent, which is distinct from being repeated.
    check!(parse("query=a").limit == 1000);
    check!(parse("query=a").line_limit == 100);

    check!(super::prelude::parse_detected_fields_params(Some("limit=5")).is_err());
    check!(super::prelude::parse_detected_fields_params(None).is_err());
}
