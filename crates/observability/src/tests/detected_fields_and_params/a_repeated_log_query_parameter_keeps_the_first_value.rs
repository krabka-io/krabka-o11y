use super::*;

/// The main query parser carries the same first-wins contract, across all
/// ten of its parameters. None of them has a default, so a repeat is the
/// only way to tell the guard from its absence.
#[test]
pub(crate) fn a_repeated_log_query_parameter_keeps_the_first_value() {
    let parse = |q: &str| super::super::prelude::parse_query_params(Some(q)).expect("valid query");

    check!(parse("query=a&query=b").query == "a");
    // A LogQL selector contains `=` itself, so the split has to take the
    // first one: taking the last would cut the value in half and leave the
    // remainder attached to the key.
    check!(
        parse(r#"query={app="web"}"#).query == r#"{app="web"}"#,
        "the value keeps its own `=`"
    );
    check!(parse("query=a&time=100&time=200").time == Some(100));
    check!(parse("query=a&start=100&start=200").start == Some(100));
    check!(parse("query=a&end=500&end=900").end == Some(500));
    check!(parse("query=a&limit=5&limit=9").limit == Some(5));
    check!(
        parse("query=a&direction=forward&direction=backward").direction
            == Some("forward".to_string())
    );
    // The four duration parameters, which the cases above never repeat.
    // Two hours against thirty minutes, so neither reading is the other.
    check!(parse("query=a&since=2h&since=30m").since == Some(7_200_000_000_000));
    check!(parse("query=a&step=2h&step=30m").step == Some(7_200_000_000_000));
    check!(parse("query=a&interval=2h&interval=30m").interval == Some(7_200_000_000_000));
    // `delay_for` reads a bare number as seconds.
    check!(parse("query=a&delay_for=1&delay_for=2").delay_for == Some(1_000_000_000));

    // Absent parameters stay absent rather than acquiring a value.
    let bare = parse("query=a");
    check!(bare.since == None);
    check!(bare.step == None);
    check!(bare.interval == None);
    check!(bare.delay_for == None);
    check!(bare.time == None);
    check!(bare.start == None);
    check!(bare.end == None);
    check!(bare.limit == None);
    check!(bare.direction == None);

    // Splitting is key-aware: an `&` only ends a parameter when a known
    // key and its `=` follow. That is what lets a LogQL query contain an
    // `&` without being truncated at it.
    check!(
        parse("query=a&direction").query == "a&direction",
        "a bare `&` is part of the value"
    );
    check!(parse("query=a&direction").direction == None);
    check!(
        parse("query=a&b&limit=5").query == "a&b",
        "and so is one followed by an unknown key"
    );
    check!(
        parse("query=a&b&limit=5").limit == Some(5),
        "the known key still splits"
    );

    // A query parameter is still required.
    check!(super::prelude::parse_query_params(Some("limit=5")).is_err());
    check!(super::prelude::parse_query_params(None).is_err());
}
