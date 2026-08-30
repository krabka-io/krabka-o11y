use super::*;

/// The detected-labels parser is first-wins on every parameter, and none
/// of them has a default that a repeat could be mistaken for. A guard
/// stuck open makes the last value win; a guard stuck shut drops the
/// parameter entirely and the default takes over -- so each is repeated
/// with a different value, and `since` uses two spans that differ from the
/// one-hour default as well as from each other.
#[test]
pub(crate) fn a_repeated_detected_labels_parameter_keeps_the_first_value() {
    let parse = |q: &str| {
        super::super::prelude::parse_detected_labels_params(Some(q)).expect("a valid query")
    };

    let params = parse(
        "query={a=\"b\"}&query={c=\"d\"}&start=100&start=200&end=900&end=800&limit=5&limit=9",
    );
    check!(params.query.as_deref() == Some("{a=\"b\"}"));
    check!(params.start == 100);
    check!(params.end == 900);
    check!(params.limit == 5);

    // `since` is read only when `start` is absent, and it sets the span
    // back from `end`. Two hours, not thirty minutes and not the one-hour
    // default.
    let params = parse("end=10000000000000&since=2h&since=30m");
    check!(params.end - params.start == 7_200_000_000_000);
}
