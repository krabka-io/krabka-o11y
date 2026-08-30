use super::*;

/// `parse_prometheus_duration` is the value-computing sibling of
/// `is_prometheus_duration_literal`: same grammar, but it sums the chunks.
/// The units must still run from larger to smaller, and a repeat is
/// refused by that same rule.
///
/// The sum is checked against a duration using several units at once,
/// since a single-unit value cannot show a chunk being dropped or
/// multiplied by the wrong scale.
#[test]
pub(crate) fn a_prometheus_duration_sums_its_chunks_in_nanoseconds() {
    let parse = super::super::prelude::parse_prometheus_duration;
    let secs = 1_000_000_000_i64;

    // Each unit's own scale.
    check!(parse("1ns") == Some(1));
    check!(parse("1us") == Some(1_000));
    check!(parse("1ms") == Some(1_000_000));
    check!(parse("1s") == Some(secs));
    check!(parse("1m") == Some(60 * secs));
    check!(parse("1h") == Some(3_600 * secs));
    check!(parse("1d") == Some(24 * 3_600 * secs));
    check!(parse("1w") == Some(7 * 24 * 3_600 * secs));
    check!(parse("1y") == Some(365 * 24 * 3_600 * secs));

    // Several units summed, so a dropped chunk changes the total.
    check!(parse("1h30m") == Some(5_400 * secs));
    check!(parse("1h1m1s") == Some(3_661 * secs));
    check!(parse("2h2m2s") == Some(7_322 * secs));
    check!(parse("1s500ms") == Some(1_500_000_000));

    // Counts above one, and zero.
    check!(parse("90s") == Some(90 * secs));
    check!(parse("0s") == Some(0));
    check!(parse("0h0m0s") == Some(0));

    // The same refusals as the literal validator.
    check!(parse("30m1h").is_none(), "out of order");
    check!(parse("1h1h").is_none(), "repeated unit");
    check!(parse("1ms1m").is_none(), "ms is the smaller unit");
    check!(parse("").is_none());
    check!(parse("1").is_none(), "no unit");
    check!(parse("h").is_none(), "no count");
    check!(parse("1x").is_none(), "unknown unit");
    check!(parse("1.5h").is_none(), "not an integer count");

    // A total that will not fit is refused rather than wrapping.
    check!(
        parse("999999999999y").is_none(),
        "overflow is not a duration"
    );
}
