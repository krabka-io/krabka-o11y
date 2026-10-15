use crate::tests::prelude::check;

trait CommonRepeatedParams {
    fn query(&self) -> &str;
    fn limit(&self) -> usize;
    fn start(&self) -> i64;
    fn end(&self) -> i64;
}

macro_rules! impl_common_repeated_params {
    ($params:ty) => {
        impl CommonRepeatedParams for $params {
            fn query(&self) -> &str {
                &self.query
            }

            fn limit(&self) -> usize {
                self.limit
            }

            fn start(&self) -> i64 {
                self.start
            }

            fn end(&self) -> i64 {
                self.end
            }
        }
    };
}

impl_common_repeated_params!(crate::tests::prelude::DetectedFieldsParams);
impl_common_repeated_params!(crate::tests::prelude::VolumeParams);

fn assert_common_repeated_fields<Params: CommonRepeatedParams>(parse: impl Fn(&str) -> Params) {
    let repeated = parse("query=a&query=b");
    check!(repeated.query() == "a");
    let repeated = parse("query=a&limit=5&limit=9");
    check!(repeated.limit() == 5);
    let repeated = parse("query=a&start=100&start=200");
    check!(repeated.start() == 100_000_000_000);
    let repeated = parse("query=a&end=500&end=900");
    check!(repeated.end() == 500_000_000_000);
}

mod detected_field_types_merge_to_what_still_describes_both {
use crate::tests::prelude::DetectedFieldType as Type;
use super::*;

/// Two sightings of the same field can disagree about its type, and the
/// merge picks what still describes both.
#[yare::parameterized(
    boolean_boolean = { Type::Boolean, Type::Boolean, Type::Boolean },
    boolean_int = { Type::Boolean, Type::Int, Type::String },
    boolean_float = { Type::Boolean, Type::Float, Type::Float },
    boolean_duration = { Type::Boolean, Type::Duration, Type::String },
    boolean_bytes = { Type::Boolean, Type::Bytes, Type::String },
    boolean_string = { Type::Boolean, Type::String, Type::String },
    int_boolean = { Type::Int, Type::Boolean, Type::String },
    int_int = { Type::Int, Type::Int, Type::Int },
    int_float = { Type::Int, Type::Float, Type::Float },
    int_duration = { Type::Int, Type::Duration, Type::String },
    int_bytes = { Type::Int, Type::Bytes, Type::String },
    int_string = { Type::Int, Type::String, Type::String },
    float_boolean = { Type::Float, Type::Boolean, Type::Float },
    float_int = { Type::Float, Type::Int, Type::Float },
    float_float = { Type::Float, Type::Float, Type::Float },
    float_duration = { Type::Float, Type::Duration, Type::Float },
    float_bytes = { Type::Float, Type::Bytes, Type::Float },
    float_string = { Type::Float, Type::String, Type::String },
    duration_boolean = { Type::Duration, Type::Boolean, Type::String },
    duration_int = { Type::Duration, Type::Int, Type::String },
    duration_float = { Type::Duration, Type::Float, Type::Float },
    duration_duration = { Type::Duration, Type::Duration, Type::Duration },
    duration_bytes = { Type::Duration, Type::Bytes, Type::String },
    duration_string = { Type::Duration, Type::String, Type::String },
    bytes_boolean = { Type::Bytes, Type::Boolean, Type::String },
    bytes_int = { Type::Bytes, Type::Int, Type::String },
    bytes_float = { Type::Bytes, Type::Float, Type::Float },
    bytes_duration = { Type::Bytes, Type::Duration, Type::String },
    bytes_bytes = { Type::Bytes, Type::Bytes, Type::Bytes },
    bytes_string = { Type::Bytes, Type::String, Type::String },
    string_boolean = { Type::String, Type::Boolean, Type::String },
    string_int = { Type::String, Type::Int, Type::String },
    string_float = { Type::String, Type::Float, Type::String },
    string_duration = { Type::String, Type::Duration, Type::String },
    string_bytes = { Type::String, Type::Bytes, Type::String },
    string_string = { Type::String, Type::String, Type::String },
)]
fn detected_field_types_merge_to_what_still_describes_both(left: Type, right: Type, want: Type) {
    check!(left.merge(right) == want, "{left:?} with {right:?}");
}
}
mod duration_unit_ratios {
use super::*;

/// The parser's unit scale is a chain of powers and calendar units.
#[yare::parameterized(
    nanosecond_to_microsecond = { "ns", "us", 1_000_i128 },
    microsecond_to_millisecond = { "us", "ms", 1_000_i128 },
    millisecond_to_second = { "ms", "s", 1_000_i128 },
    second_to_minute = { "s", "m", 60_i128 },
    minute_to_hour = { "m", "h", 60_i128 },
    hour_to_day = { "h", "d", 24_i128 },
    day_to_week = { "d", "w", 7_i128 },
    day_to_year = { "d", "y", 365_i128 },
)]
fn duration_unit_ratios_match_expected_scale(smaller: &str, larger: &str, ratio: i128) {
    let nanoseconds = |name| {
        crate::tests::prelude::prometheus_duration_unit(name)
            .expect("known unit")
            .nanoseconds
    };
    check!(nanoseconds(larger) == ratio * nanoseconds(smaller));
}
}
/// The detected-fields parser carries the same first-wins contract.
#[test]
pub(crate) fn a_repeated_detected_fields_parameter_keeps_the_first_value() {
    let parse = |q: &str| {
        crate::tests::prelude::parse_detected_fields_params(Some(q)).expect("valid query")
    };

    crate::tests::detected_fields_and_params::assert_common_repeated_fields(parse);
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

    check!(crate::tests::prelude::parse_detected_fields_params(Some("limit=5")).is_err());
    check!(crate::tests::prelude::parse_detected_fields_params(None).is_err());
}

/// The detected-labels parser is first-wins on every parameter, and none
/// of them has a default that a repeat could be mistaken for. A guard
/// stuck open makes the last value win; a guard stuck shut drops the
/// parameter entirely and the default takes over -- so each is repeated
/// with a different value, and `since` uses two spans that differ from the
/// one-hour default as well as from each other.
#[test]
pub(crate) fn a_repeated_detected_labels_parameter_keeps_the_first_value() {
    let parse = |q: &str| {
        crate::tests::prelude::parse_detected_labels_params(Some(q)).expect("a valid query")
    };

    let params = parse(
        "query={a=\"b\"}&query={c=\"d\"}&start=100&start=200&end=900&end=800&limit=5&limit=9",
    );
    check!(params.query.as_deref() == Some("{a=\"b\"}"));
    check!(params.start == 100_000_000_000);
    check!(params.end == 900_000_000_000);
    check!(params.limit == 5);

    // `since` is read only when `start` is absent, and it sets the span
    // back from `end`. Two hours, not thirty minutes and not the one-hour
    // default.
    let params = parse("end=10000000000000&since=2h&since=30m");
    check!(params.end - params.start == 7_200_000_000_000);
}

/// The main query parser carries the same first-wins contract, across all
/// ten of its parameters. None of them has a default, so a repeat is the
/// only way to tell the guard from its absence.
#[test]
pub(crate) fn a_repeated_log_query_parameter_keeps_the_first_value() {
    let parse = |q: &str| crate::tests::prelude::parse_query_params(Some(q)).expect("valid query");

    check!(parse("query=a&query=b").query == "a");
    // A LogQL selector contains `=` itself, so the split has to take the
    // first one: taking the last would cut the value in half and leave the
    // remainder attached to the key.
    check!(
        parse(r#"query={app="web"}"#).query == r#"{app="web"}"#,
        "the value keeps its own `=`"
    );
    check!(parse("query=a&time=100&time=200").time == Some(100_000_000_000));
    check!(parse("query=a&time=01").time == Some(1_000_000_000_000_000_000));
    check!(parse("query=a&time=0.5").time == Some(500_000_000_000_000_000));
    check!(parse("query=a&time=0").time == Some(0));
    check!(
        crate::tests::prelude::parse_query_params(Some("query=a&time=9.223372037"))
            .unwrap()
            .time
            == Some(9_223_000_000_000_000_000)
    );
    check!(parse("query=a&start=100&start=200").start == Some(100_000_000_000));
    check!(parse("query=a&end=500&end=900").end == Some(500_000_000_000));
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
    check!(crate::tests::prelude::parse_query_params(Some("limit=5")).is_err());
    check!(crate::tests::prelude::parse_query_params(None).is_err());
}

/// A repeated query parameter keeps its first value and ignores the rest.
///
/// Each arm of the parse loop is guarded on the field still being unset, so
/// a second occurrence falls through to the catch-all and is dropped. With
/// the guard gone the last occurrence would win instead, which no test
/// passing a well-formed query once can tell apart -- the values have to
/// differ and the query has to repeat.
#[test]
pub(crate) fn a_repeated_volume_parameter_keeps_the_first_value() {
    let parse = |q: &str| crate::tests::prelude::parse_volume_params(Some(q)).expect("valid query");

    crate::tests::detected_fields_and_params::assert_common_repeated_fields(parse);
    check!(parse("query=a&step=5s&step=9s").step == parse("query=a&step=5s").step);
    check!(
        parse("query=a&targetLabels=x&targetLabels=y").target_labels == Some(vec!["x".to_string()])
    );
    check!(matches!(
        parse("query=a&aggregateBy=labels&aggregateBy=series").aggregate_by,
        crate::tests::prelude::VolumeAggregateBy::Labels
    ));

    // The defaults still apply when a parameter is absent entirely, which
    // is a different thing from being repeated.
    check!(parse("query=a").limit == 100);
    check!(matches!(
        parse("query=a").aggregate_by,
        crate::tests::prelude::VolumeAggregateBy::Series
    ));
    check!(parse("query=a").target_labels == None);

    // An empty label in the list is dropped rather than kept as "".
    check!(
        parse("query=a&targetLabels=x,,y").target_labels
            == Some(vec!["x".to_string(), "y".to_string()])
    );

    // A query with no `query` at all is an error, not a default.
    check!(crate::tests::prelude::parse_volume_params(Some("limit=5")).is_err());
    check!(crate::tests::prelude::parse_volume_params(None).is_err());
    // An unknown aggregation is rejected rather than falling back.
    check!(
        crate::tests::prelude::parse_volume_params(Some("query=a&aggregateBy=nonsense")).is_err()
    );
}

/// Both duration-unit tables use one ordinal/bit contract; the Prometheus
/// table also exposes each unit's nanosecond value.
#[yare::parameterized(
    year = { "y", 0_u8, 31_536_000_000_000_000_i128 },
    week = { "w", 1_u8, 604_800_000_000_000_i128 },
    day = { "d", 2_u8, 86_400_000_000_000_i128 },
    hour = { "h", 3_u8, 3_600_000_000_000_i128 },
    minute = { "m", 4_u8, 60_000_000_000_i128 },
    second = { "s", 5_u8, 1_000_000_000_i128 },
    millisecond = { "ms", 6_u8, 1_000_000_i128 },
    microsecond = { "us", 7_u8, 1_000_i128 },
    nanosecond = { "ns", 8_u8, 1_i128 },
)]
fn duration_unit_maps_to_ordinal_bit_and_nanoseconds(name: &str, ordinal: u8, nanoseconds: i128) {
    let expected = crate::querier::analytics::detected_fields::DetectedDurationUnit {
        order: ordinal,
        bit: 1_u16 << ordinal,
    };
    check!(crate::tests::prelude::detected_duration_unit(name) == Some(expected));
    let prometheus = crate::tests::prelude::prometheus_duration_unit(name).unwrap();
    check!(prometheus.rank == ordinal);
    check!(prometheus.bit == expected.bit);
    check!(prometheus.nanoseconds == nanoseconds);
}

#[test]
fn duration_unit_tables_reject_unknown_and_case_changed_units() {
    for unit in ["", "Y", "mo", "sec", "S"] {
        check!(crate::tests::prelude::detected_duration_unit(unit) == None);
        check!(crate::tests::prelude::prometheus_duration_unit(unit) == None);
    }
}

/// `parse_logfmt_pairs` walks a logfmt line byte by byte: whitespace
/// separates pairs, `=` separates a key from its value, and a quoted value
/// may contain both. Every case below fixes one decision that boundary
/// takes, since a parser that is off by one still returns pairs -- just
/// the wrong ones.
#[test]
pub(crate) fn logfmt_pairs_split_on_unquoted_whitespace() {
    let parse = crate::tests::prelude::parse_logfmt_pairs;
    let pair = |k: &str, v: &str| crate::querier::analytics::detected_fields::ParsedLogfmtPair {
        name: k.to_string(),
        value: v.to_string(),
    };

    check!(parse("a=1") == vec![pair("a", "1")]);
    // An unquoted value carrying letters, so a transformation of the slice
    // is visible: digits alone survive most of them unchanged.
    check!(parse("level=warn") == vec![pair("level", "warn")]);
    check!(parse("msg=hello level=warn") == vec![pair("msg", "hello"), pair("level", "warn")]);
    check!(parse("a=1 b=2") == vec![pair("a", "1"), pair("b", "2")]);
    check!(
        parse("  a=1   b=2  ") == vec![pair("a", "1"), pair("b", "2")],
        "runs of whitespace are separators, not content"
    );
    check!(parse("") == vec![], "an empty line has no pairs");
    check!(parse("   ") == vec![], "nor does whitespace alone");

    // A key with nothing after the `=` is a pair with an empty value,
    // which is not the same as the key being absent.
    check!(parse("a=") == vec![pair("a", "")]);
    check!(parse("a= b=2") == vec![pair("a", ""), pair("b", "2")]);

    // A bare token is not a pair and must not swallow the next one.
    check!(parse("bare a=1") == vec![pair("a", "1")]);
    check!(parse("a=1 bare") == vec![pair("a", "1")]);
    check!(parse("bare") == vec![]);

    // A leading `=` has an empty key, which is skipped rather than
    // recorded under an empty name.
    check!(parse("=1 a=2") == vec![pair("a", "2")]);

    // Quoted values hold what unquoted ones cannot.
    check!(
        parse(r#"a="x y""#) == vec![pair("a", "x y")],
        "whitespace inside quotes"
    );
    check!(parse(r#"a="x y" b=2"#) == vec![pair("a", "x y"), pair("b", "2")]);
    // An escape inside a quoted value. Every other quoted case here is
    // escape-free, so the two steps the escape branch takes -- over the
    // backslash and over what it protects -- were never taken at all.
    check!(
        parse(r#"a="x \"y\" z""#) == vec![pair("a", r#"x "y" z"#)],
        "an escaped quote is content, not the end of the value"
    );
    check!(
        parse(r#"a="x\\y""#) == vec![pair("a", r"x\y")],
        "an escaped backslash is one backslash"
    );
    // A backslash with nothing after it is not an escape: there is no
    // second byte to step over.
    check!(parse("a=\"x\\") == vec![pair("a", "x\\")]);

    check!(
        parse(r#"a="""#) == vec![pair("a", "")],
        "an empty quoted value"
    );
    check!(
        parse(r#"a="x\"y" b=2"#) == vec![pair("a", "x\"y"), pair("b", "2")],
        "an escaped quote does not end the value"
    );
    check!(
        parse(r#"a="x\\y""#) == vec![pair("a", "x\\y")],
        "an escaped backslash is one backslash"
    );

    // An unterminated quote runs to the end of the line rather than
    // dropping the pair.
    check!(parse(r#"a="x y"#) == vec![pair("a", "x y")]);
    // A trailing backslash has nothing to escape and is taken literally.
    check!(parse(r#"a="x\"#) == vec![pair("a", "x\\")]);
}

/// `ScalarSample::compare` orders two rationals by cross-multiplication,
/// so the fractions below are chosen not to be decided by their numerators
/// alone: 1/2 against 2/3 orders one way and 2/3 against 1/2 the other,
/// while 1/2 and 2/4 are equal without being identical. A comparison that
/// forgot to cross-multiply would still get many pairs right.
#[test]
pub(crate) fn scalar_samples_compare_as_rationals() {
    use crate::tests::prelude::{ScalarComparisonOp as Op, ScalarSample};

    let cmp = |n1: i128, d1: u128, op, n2: i128, d2: u128| {
        ScalarSample::new(n1, d1).compare(op, ScalarSample::new(n2, d2))
    };

    // 1/2 < 2/3, which no comparison of numerators alone would decide.
    check!(cmp(1, 2, Op::Less, 2, 3) == Some(true));
    check!(cmp(1, 2, Op::Greater, 2, 3) == Some(false));
    check!(cmp(2, 3, Op::Greater, 1, 2) == Some(true));

    // Equal values with different representations.
    check!(cmp(1, 2, Op::Equal, 2, 4) == Some(true));
    check!(cmp(1, 2, Op::NotEqual, 2, 4) == Some(false));
    check!(
        cmp(1, 2, Op::LessOrEqual, 2, 4) == Some(true),
        "equal satisfies <="
    );
    check!(cmp(1, 2, Op::GreaterOrEqual, 2, 4) == Some(true), "and >=");
    check!(cmp(1, 2, Op::Less, 2, 4) == Some(false), "but not <");
    check!(cmp(1, 2, Op::Greater, 2, 4) == Some(false), "nor >");

    // Each strict operator against its non-strict twin, on a pair that is
    // not equal, so the two cannot be confused for one another.
    check!(cmp(1, 3, Op::Less, 1, 2) == Some(true));
    check!(cmp(1, 3, Op::LessOrEqual, 1, 2) == Some(true));
    check!(cmp(1, 2, Op::Greater, 1, 3) == Some(true));
    check!(cmp(1, 2, Op::GreaterOrEqual, 1, 3) == Some(true));

    // Signs, including a negative on either side of zero.
    check!(cmp(-1, 2, Op::Less, 1, 2) == Some(true));
    check!(
        cmp(-1, 2, Op::Less, -1, 3) == Some(true),
        "-1/2 is below -1/3"
    );
    check!(cmp(-1, 2, Op::Equal, -2, 4) == Some(true));
    check!(
        cmp(0, 1, Op::Equal, 0, 5) == Some(true),
        "zero is zero at any scale"
    );

    // A product that cannot fit answers nothing rather than wrapping.
    check!(cmp(i128::MAX, 1, Op::Greater, 1, 2) == None);
}
