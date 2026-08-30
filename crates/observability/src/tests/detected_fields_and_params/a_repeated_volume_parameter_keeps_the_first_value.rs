use super::*;

/// A repeated query parameter keeps its first value and ignores the rest.
///
/// Each arm of the parse loop is guarded on the field still being unset, so
/// a second occurrence falls through to the catch-all and is dropped. With
/// the guard gone the last occurrence would win instead, which no test
/// passing a well-formed query once can tell apart -- the values have to
/// differ and the query has to repeat.
#[test]
pub(crate) fn a_repeated_volume_parameter_keeps_the_first_value() {
    let parse = |q: &str| super::super::prelude::parse_volume_params(Some(q)).expect("valid query");

    check!(parse("query=a&query=b").query == "a");
    check!(parse("query=a&limit=5&limit=9").limit == 5);
    check!(parse("query=a&start=100&start=200").start == 100);
    check!(parse("query=a&end=500&end=900").end == 500);
    check!(parse("query=a&step=5s&step=9s").step == parse("query=a&step=5s").step);
    check!(
        parse("query=a&targetLabels=x&targetLabels=y").target_labels == Some(vec!["x".to_string()])
    );
    check!(matches!(
        parse("query=a&aggregateBy=labels&aggregateBy=series").aggregate_by,
        super::super::prelude::VolumeAggregateBy::Labels
    ));

    // The defaults still apply when a parameter is absent entirely, which
    // is a different thing from being repeated.
    check!(parse("query=a").limit == 100);
    check!(matches!(
        parse("query=a").aggregate_by,
        super::super::prelude::VolumeAggregateBy::Series
    ));
    check!(parse("query=a").target_labels == None);

    // An empty label in the list is dropped rather than kept as "".
    check!(
        parse("query=a&targetLabels=x,,y").target_labels
            == Some(vec!["x".to_string(), "y".to_string()])
    );

    // A query with no `query` at all is an error, not a default.
    check!(super::super::prelude::parse_volume_params(Some("limit=5")).is_err());
    check!(super::super::prelude::parse_volume_params(None).is_err());
    // An unknown aggregation is rejected rather than falling back.
    check!(
        super::super::prelude::parse_volume_params(Some("query=a&aggregateBy=nonsense")).is_err()
    );
}
