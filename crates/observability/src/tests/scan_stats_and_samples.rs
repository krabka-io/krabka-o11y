use super::prelude::METRIC_DECIMAL_SCALE;
use super::prelude::{BTreeMap, Labels, MetricValue, check};
/// `populate_loki_query_scan_stats` fills Loki's stats block, and the two
/// per-source sections appear only when that source contributed. An empty
/// `ingester` or `store` object would tell a client the source was
/// consulted and returned nothing, which is a different claim from not
/// having been consulted -- Grafana renders the two differently.
///
/// The summary is unconditional and sums BOTH sources, so it is checked
/// with each source alone as well as with both: with one contributing,
/// a sum that dropped the other term still reads correctly.
#[test]
pub(crate) fn loki_scan_stats_report_only_the_sources_that_contributed() {
    let fill = |store_lines, ingester_lines, chunks| {
        let mut stats = serde_json::json!({});
        super::prelude::populate_loki_query_scan_stats(
            &mut stats,
            krabka_units::bytes(4_096),
            store_lines,
            ingester_lines,
            chunks,
        );
        stats
    };

    // Both sources contributed.
    let both = fill(7, 3, 2);
    check!(both["ingester"]["decompressedLines"] == 3);
    check!(both["ingester"]["totalLinesSent"] == 3);
    check!(both["store"]["decompressedLines"] == 7);
    check!(both["store"]["totalChunksRef"] == 2);
    check!(both["store"]["totalChunksDownloaded"] == 2);
    check!(both["store"]["compressedBytes"] == 4_096);
    check!(both["store"]["decompressedBytes"] == 4_096);
    check!(both["summary"]["totalBytesProcessed"] == 4_096);
    check!(
        both["summary"]["totalLinesProcessed"] == 10,
        "the summary sums store and ingester"
    );

    // Only the ingester: no store section at all, not an empty one.
    let hot = fill(0, 3, 0);
    check!(hot["ingester"]["decompressedLines"] == 3);
    check!(hot.get("store").is_none(), "absent, not empty");
    check!(hot["summary"]["totalLinesProcessed"] == 3);

    // Only the store: no ingester section.
    let cold = fill(7, 0, 2);
    check!(cold["store"]["decompressedLines"] == 7);
    check!(cold.get("ingester").is_none(), "absent, not empty");
    check!(cold["summary"]["totalLinesProcessed"] == 7);

    // Neither: the summary still reports, at zero.
    let empty = fill(0, 0, 0);
    check!(empty.get("store").is_none());
    check!(empty.get("ingester").is_none());
    check!(empty["summary"]["totalLinesProcessed"] == 0);
    check!(
        empty["summary"]["totalBytesProcessed"] == 4_096,
        "bytes are unconditional"
    );

    // The store section is gated on CHUNKS, not on lines: a chunk that
    // matched no lines was still downloaded and still cost bytes.
    let scanned_nothing = fill(0, 0, 2);
    check!(scanned_nothing["store"]["totalChunksRef"] == 2);
    check!(scanned_nothing["store"]["decompressedLines"] == 0);
}

/// `parse_decimal_seconds_timestamp` reads "seconds.fraction" as
/// nanoseconds. It REQUIRES the point -- a bare integer is handled
/// elsewhere, as seconds or as nanos depending on context, and guessing
/// here would pre-empt that. The fraction is padded to nine places and
/// truncated past them, so a microsecond timestamp scales correctly.
///
/// The `take(9)` bounding that loop is belt-and-braces: the scale divides
/// by ten each digit and reaches zero by integer division after the ninth,
/// so a tenth digit contributes nothing whether it is read or not.
/// Widening the take is an equivalent mutation.
#[test]
pub(crate) fn a_decimal_seconds_timestamp_scales_its_fraction_to_nanos() {
    let parse = super::prelude::parse_decimal_seconds_timestamp;

    // The fraction is positional: one digit is tenths, not nanos.
    check!(parse("5.5") == Some(5_500_000_000));
    check!(parse("5.05") == Some(5_050_000_000));
    check!(parse("0.000000001") == Some(1), "one nanosecond");
    check!(parse("1.000000000") == Some(1_000_000_000));

    // Past nine places the rest is dropped rather than rounded.
    check!(
        parse("0.0000000009") == Some(0),
        "a tenth of a nanosecond is lost"
    );
    check!(parse("1.9999999999") == Some(1_999_999_999));

    // Either side may be empty, but not both.
    check!(parse(".5") == Some(500_000_000));
    check!(parse("5.") == Some(5_000_000_000));
    check!(parse(".").is_none());

    // Signs, including a negative instant.
    check!(parse("-5.5") == Some(-5_500_000_000));
    check!(parse("+5.5") == Some(5_500_000_000));

    // The point is required: a bare integer is somebody else's problem.
    check!(parse("5").is_none(), "no point, no answer");
    check!(parse("").is_none());
    check!(parse("abc").is_none());
    check!(parse("5.abc").is_none());
    check!(parse("5.5.5").is_none(), "the second point is not a digit");
}

/// `metric_binary_sample_timestamp_ns_candidates` offers every reading a
/// sample's timestamp could plausibly have. Which readings depends on how
/// it was encoded, and each JSON type takes its own branch: an integer is
/// ambiguous and offers two, a float is seconds and offers one, a string
/// may parse either way and offers whichever succeed.
#[test]
pub(crate) fn a_sample_timestamp_offers_every_reading_its_encoding_allows() {
    let candidates = |timestamp: serde_json::Value| {
        super::prelude::metric_binary_sample_timestamp_ns_candidates(&serde_json::json!([
            timestamp, "1"
        ]))
    };

    // An integer is ambiguous: both the raw value and it read as seconds.
    check!(candidates(serde_json::json!(5)) == Some(vec![5, 5_000_000_000]));
    // Zero collapses to one reading, since both are the same number.
    check!(candidates(serde_json::json!(0)) == Some(vec![0]));

    // A float is seconds, rounded to the nearest nanosecond, and offers
    // only that -- there is no second reading to be ambiguous about.
    check!(candidates(serde_json::json!(5.5)) == Some(vec![5_500_000_000]));
    // Rounded, not truncated. 5.5 lands on a whole nanosecond and cannot
    // show the difference; 1.7 nanoseconds rounds up to 2 where flooring
    // gives 1, which is the sub-nanosecond precision a float carries and
    // an integer count cannot.
    check!(candidates(serde_json::json!(1.7e-9)) == Some(vec![2]));

    // A string is tried both ways and offers whichever parse. "5" has no
    // decimal point so only the integer reading applies; "5.5" is the
    // reverse.
    check!(candidates(serde_json::json!("5")) == Some(vec![5, 5_000_000_000]));
    check!(candidates(serde_json::json!("5.5")) == Some(vec![5_500_000_000]));

    // Nothing parses, or there is nothing to parse.
    check!(candidates(serde_json::json!("nonsense")).is_none());
    check!(candidates(serde_json::json!(true)).is_none());
    check!(
        super::prelude::metric_binary_sample_timestamp_ns_candidates(&serde_json::json!([]))
            .is_none()
    );
    check!(
        super::prelude::metric_binary_sample_timestamp_ns_candidates(&serde_json::json!("bare"))
            .is_none()
    );
}

/// Two samples share an instant if any of their candidate readings agree.
/// A bare integer is ambiguous -- Prometheus writes timestamps in seconds
/// and Loki in nanoseconds -- so each yields both readings, and 5 matches
/// `5_000_000_000` because they are the same moment spelled differently.
/// That is the whole reason the comparison is over LISTS rather than
/// values, and a fixture using one spelling throughout never shows it.
#[test]
pub(crate) fn two_samples_share_an_instant_if_any_reading_of_them_agrees() {
    let matches =
        |left, right| super::prelude::metric_binary_sample_timestamps_match(&left, &right);
    let at = |timestamp: serde_json::Value| serde_json::json!([timestamp, "1"]);

    // The same number, and the same instant written two ways.
    check!(matches(at(serde_json::json!(5)), at(serde_json::json!(5))));
    check!(
        matches(
            at(serde_json::json!(5)),
            at(serde_json::json!(5_000_000_000_i64))
        ),
        "seconds and nanoseconds for the same moment"
    );
    check!(
        matches(
            at(serde_json::json!(5_000_000_000_i64)),
            at(serde_json::json!(5))
        ),
        "and the other way round"
    );

    // Different instants, in either spelling.
    check!(!matches(at(serde_json::json!(5)), at(serde_json::json!(7))));
    check!(!matches(
        at(serde_json::json!(5)),
        at(serde_json::json!(7_000_000_000_i64))
    ));

    // Neither side parses: they fall back to comparing the raw values, so
    // two identical unparseable timestamps still pair up and two different
    // ones do not.
    check!(matches(
        at(serde_json::json!("nonsense")),
        at(serde_json::json!("nonsense"))
    ));
    check!(!matches(
        at(serde_json::json!("nonsense")),
        at(serde_json::json!("other"))
    ));

    // One side parses and the other does not: no match, rather than
    // falling through to a raw comparison that would never agree anyway.
    check!(!matches(
        at(serde_json::json!(5)),
        at(serde_json::json!("nonsense"))
    ));
    check!(!matches(
        at(serde_json::json!("nonsense")),
        at(serde_json::json!(5))
    ));
}

/// `format_metric_value` renders a rational as a decimal, capped at nine
/// places and with trailing zeros trimmed. A whole number gets no decimal
/// point at all, which is a different branch from one whose decimals all
/// trim away -- both are checked, since they produce the same text by
/// different routes.
#[test]
pub(crate) fn a_metric_value_renders_without_trailing_zeros() {
    let render = |numerator, denominator| {
        super::prelude::format_metric_value(MetricValue::new(numerator, denominator))
    };

    // Whole numbers take the early return and carry no point.
    check!(render(5, 1) == "5");
    check!(render(0, 1) == "0");
    check!(render(-5, 1) == "-5");
    // A fraction that reduces to a whole number takes the same branch.
    check!(render(10, 5) == "2");

    // Exact decimals keep only the digits they need.
    check!(render(1, 2) == "0.5");
    check!(render(-1, 2) == "-0.5");
    check!(render(1, 4) == "0.25");
    check!(render(3, 2) == "1.5");
    check!(render(-3, 2) == "-1.5");

    // The sign is on the whole part, and survives a zero whole part --
    // "-0.5" rather than "0.5" with the minus lost on the way through
    // `unsigned_abs`.
    check!(render(-1, 4) == "-0.25");

    // A repeating fraction is cut at nine places, not rounded up: a third
    // is nine 3s, and two thirds is nine 6s rather than ...667.
    check!(render(1, 3) == "0.333333333");
    check!(render(2, 3) == "0.666666666");

    // Trailing zeros are trimmed even when the division produces them.
    check!(render(1, 8) == "0.125");
    check!(render(1, 5) == "0.2", "not 0.200000000");

    // The trim only has anything to do when the nine-digit cap lands on a
    // zero: a terminating fraction stops as soon as the remainder does, so
    // it never appends one. 1/11 is 0.090909090... -- nine digits ending
    // in a zero that must come off.
    check!(render(1, 11) == "0.09090909");
}

/// `strip_outer_parenthesized_expression` unwraps a query that is wholly
/// parenthesised, and refuses one that merely starts and ends with
/// brackets belonging to different groups -- "(a)+(b)" is not a
/// parenthesised expression, and unwrapping it would produce "a)+(b".
#[test]
pub(crate) fn only_a_wholly_parenthesised_expression_is_unwrapped() {
    let strip = super::prelude::strip_outer_parenthesized_expression;

    check!(strip("(a)") == Some("a"));
    check!(strip("  (a)  ") == Some("a"), "the query is trimmed first");
    check!(strip("( a )") == Some("a"), "and so are the contents");
    check!(strip("((a))") == Some("(a)"), "one layer at a time");
    check!(strip("(a+b)") == Some("a+b"));

    // The brackets must be the SAME pair. This is the case that a naive
    // starts-with/ends-with check gets wrong.
    check!(strip("(a)+(b)").is_none());
    check!(strip("(a)(b)").is_none());

    // Not parenthesised at all. "a(b)" matters most: it ends with a
    // bracket whose opener is not the first character, so a precheck
    // requiring only ONE of the two ends to match would unwrap it to the
    // nonsense "(b".
    check!(strip("a(b)").is_none());
    check!(strip("a").is_none());
    check!(strip("(a").is_none());
    check!(strip("a)").is_none());
    check!(strip("").is_none());

    // Unbalanced inside. Note the `checked_sub` guarding the depth counter
    // is unreachable: a leading `)` would need the opening precheck to have
    // passed, which requires a leading `(`. Replacing it with a saturating
    // subtraction is an equivalent mutation, not a gap.
    check!(strip("(a))").is_none());
    check!(strip("((a)").is_none());

    // A parenthesis inside a string is text, not structure.
    check!(strip(r#"({app="("})"#) == Some(r#"{app="("}"#));
}

/// `MetricValue::sqrt` returns zero rather than an error for anything with
/// no real root, and it FLOORS to nine decimal places rather than rounding
/// -- so an irrational root is truncated, not nudged up. A NaN reaching a
/// series would poison every aggregation over it.
///
/// The `!is_finite() || <= 0.0` guard cannot be tested from outside, and
/// is kept for what it says rather than what it does: every input it
/// catches also reaches zero through the fall-through, because
/// `i128::from_f64(NaN)` defaults to 0 and `MetricValue::new` maps a zero
/// numerator to zero. Relaxing or removing the guard is an equivalent
/// mutation. It stays because it states the intent -- no real root means
/// zero -- where the fall-through only arrives there by accident.
#[test]
pub(crate) fn a_metric_square_root_floors_and_refuses_what_has_no_root() {
    let value = |numerator, denominator| MetricValue::new(numerator, denominator);

    check!(value(4, 1).sqrt() == value(2, 1));
    check!(value(9, 1).sqrt() == value(3, 1));
    check!(value(1, 4).sqrt() == value(1, 2), "a fractional root");

    // sqrt(2) is irrational: floored at nine places, not rounded. The
    // tenth digit is a 3, so flooring and rounding agree here -- and
    // sqrt(3) at 1.732050807... has a 5 next, where they differ.
    check!(value(2, 1).sqrt() == MetricValue::new(1_414_213_562, METRIC_DECIMAL_SCALE));
    check!(value(3, 1).sqrt() == MetricValue::new(1_732_050_807, METRIC_DECIMAL_SCALE));

    // Zero and negatives have no positive root, and both answer zero
    // rather than propagating a NaN into the series.
    check!(value(0, 1).sqrt() == MetricValue::zero());
    check!(value(-4, 1).sqrt() == MetricValue::zero());
    check!(value(-1, 1).sqrt() == MetricValue::zero());
}

/// `MetricValue::subtract` is exact rational arithmetic, so it must not
/// round-trip through a float. The operands are chosen with different
/// denominators, since equal ones let the cross-multiplication cancel out
/// and hide a swapped operand.
#[test]
pub(crate) fn a_metric_subtraction_stays_exact_across_denominators() {
    let value = |numerator, denominator| MetricValue::new(numerator, denominator);

    check!(value(5, 1).subtract(value(3, 1)) == value(2, 1));
    check!(
        value(3, 1).subtract(value(5, 1)) == value(-2, 1),
        "and the other way"
    );

    // 1/2 - 1/3 is exactly 1/6, which no float can hold.
    check!(value(1, 2).subtract(value(1, 3)) == value(1, 6));
    check!(value(1, 3).subtract(value(1, 2)) == value(-1, 6));

    // Subtracting from itself is zero however it is spelled.
    check!(value(7, 3).subtract(value(7, 3)) == MetricValue::zero());
    check!(value(2, 4).subtract(value(1, 2)) == MetricValue::zero());
}

/// `sort_loki_stream_values` orders each stream's entries by timestamp.
/// The timestamps are decimal strings, so a lexicographic sort would put
/// "1000" before "999" -- the fixture crosses that boundary deliberately.
/// An unparseable timestamp sorts last rather than first, so a malformed
/// entry does not claim to be the oldest line in the stream.
#[test]
pub(crate) fn loki_stream_values_sort_numerically_not_lexicographically() {
    let entry = |timestamp: &str| [timestamp.to_string(), "line".to_string()];
    let mut streams = BTreeMap::new();
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    streams.insert(
        labels.clone(),
        vec![
            entry("1000"),
            entry("999"),
            entry("nonsense"),
            entry("10000"),
            entry("2"),
        ],
    );

    super::prelude::sort_loki_stream_values(&mut streams);

    let order = streams[&labels]
        .iter()
        .map(|[timestamp, _]| timestamp.as_str())
        .collect::<Vec<_>>();
    check!(
        order == vec!["2", "999", "1000", "10000", "nonsense"],
        "numeric order, with the unparseable entry last"
    );
}

/// `decode_form_component` decodes one `application/x-www-form-urlencoded`
/// field: `+` is a space, `%XX` is a byte, and everything else is itself.
/// A truncated or malformed escape is an error rather than a literal `%`,
/// and the decoded bytes still have to be UTF-8 -- a valid escape can name
/// a byte that is not.
#[test]
pub(crate) fn a_form_component_decodes_its_escapes_or_refuses_them() {
    let decode = |value: &str| super::prelude::decode_form_component(value).ok();

    check!(decode("plain") == Some("plain".to_string()));
    check!(decode("") == Some(String::new()));
    check!(decode("a+b") == Some("a b".to_string()), "plus is a space");
    check!(decode("a%20b") == Some("a b".to_string()), "and so is %20");
    check!(decode("%2F") == Some("/".to_string()));
    check!(
        decode("%2f") == Some("/".to_string()),
        "hex is case-insensitive"
    );
    check!(
        decode("%C3%A9") == Some("\u{e9}".to_string()),
        "a multi-byte character"
    );

    // A `%` that does not introduce two hex digits is an error, not a
    // literal percent sign -- at the end of the string and mid-string.
    check!(decode("a%").is_none());
    check!(decode("a%2").is_none());
    check!(decode("a%ZZb").is_none());
    check!(decode("100%").is_none());

    // A well-formed escape naming a byte that is not valid UTF-8.
    check!(decode("%FF").is_none());
}

/// `has_word_boundary` asks whether a match at `index` stands alone rather
/// than sitting inside a longer word. Both sides have to hold, so each is
/// broken on its own -- and the ends of the string count as boundaries,
/// which is what `is_none_or` is doing there.
#[test]
pub(crate) fn a_word_boundary_needs_whitespace_or_an_end_on_both_sides() {
    let boundary = super::prelude::has_word_boundary;

    check!(boundary("a and b", 2, 3), "space either side");
    check!(boundary("and", 0, 3), "both ends of the string");
    check!(boundary("and b", 0, 3), "the start, and a space after");
    check!(boundary("a and", 2, 3), "a space before, and the end");

    // Each side broken on its own.
    check!(!boundary("aand b", 1, 3), "no boundary before");
    check!(!boundary("a andb", 2, 3), "no boundary after");
    check!(!boundary("aandb", 1, 3), "neither side");
}

/// `line_number` counts the newlines before a position, one-based, and
/// clamps a position past the end rather than panicking on it -- a parse
/// error can report a position at the very end of the input.
#[test]
pub(crate) fn a_line_number_counts_from_one_and_clamps_past_the_end() {
    let line = super::prelude::line_number;

    check!(line("abc", 0) == 1, "the first line is one, not zero");
    check!(line("abc", 3) == 1);
    check!(line("a\nb", 0) == 1);
    check!(line("a\nb", 2) == 2, "past the newline");
    check!(line("a\nb", 1) == 1, "the newline itself is still line one");
    check!(line("a\n\nb", 3) == 3, "a blank line counts");
    check!(line("a\nb", 99) == 2, "a position past the end clamps");
    check!(line("", 0) == 1);
    check!(line("", 99) == 1);
}
