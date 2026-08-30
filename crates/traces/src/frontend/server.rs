//! axum HTTP surface for the query-frontend.
//!
//! It covers the Tempo query endpoints, tenant extraction, the v2 by-id
//! `status`/`message` envelope, and time-param parsing that matches the
//! querier's contract. `start` and `end` are epoch **seconds**, and a
//! fractional part is allowed.
//!
//! The router is generic over the backend and catalog pair. Tests therefore
//! drive `MockQuerier` with `MockCatalog`, and production binds `HttpQuerier`
//! with `TraceIndexCatalog`.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::json;

use crate::frontend::{
    QueryFrontend,
    backend::{BackendError, QuerierBackend},
    job::BlockCatalog,
    merge::TraceStatus,
    wire::parse_hex16,
};

// --- param helpers (mirror the querier's contract) --------------------------

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// The frontend's query parameters each have a boundary that only one
    /// input distinguishes: an empty window is legal but an inverted one is
    /// not, and a step must be strictly positive rather than merely parsed.
    #[test]
    fn frontend_time_bounds_and_step_reject_only_what_they_should() {
        let uri = |query: &str| {
            format!("http://x/api?{query}")
                .parse::<Uri>()
                .expect("a valid uri")
        };

        // end == start is an empty window and allowed, so `<` must not become
        // `<=`; end < start is refused, so it must not become `==` either.
        check!(super::optional_time_bounds(&uri("start=5&end=5")).is_ok());
        check!(super::optional_time_bounds(&uri("start=5&end=4")).is_err());
        check!(super::optional_time_bounds(&uri("start=5&end=6")).is_ok());
        check!(super::optional_time_bounds(&uri("")) == Ok((0, i64::MAX)));

        // A step is required, must parse, and must be strictly positive.
        check!(super::required_step(&uri("step=5s")) == Ok(5_000_000_000));
        check!(
            super::required_step(&uri("step=0")).is_err(),
            "zero is not positive"
        );
        check!(
            super::required_step(&uri("step=-1s")).is_err(),
            "nor is a negative step"
        );
        check!(
            super::required_step(&uri("")).is_err(),
            "and it is not optional"
        );
        check!(super::required_step(&uri("step=abc")).is_err());

        // The scope is optional, but an unrecognised name is refused rather
        // than quietly treated as absent.
        check!(super::scope_param(&uri("scope=span")) == Ok(Some(krabka_traceql::TagScope::Span)));
        check!(
            super::scope_param(&uri("scope=resource"))
                == Ok(Some(krabka_traceql::TagScope::Resource))
        );
        check!(super::scope_param(&uri("")) == Ok(None));
        check!(super::scope_param(&uri("scope=nonsense")).is_err());
    }

    /// `parse_duration_component_ns` scales one number by its unit. The
    /// fraction is divided by ten to its own length, so a two-digit fraction
    /// is hundredths -- which only shows when the fraction's length and its
    /// value differ, hence the ".05" case beside the ".5" one.
    #[test]
    fn a_duration_component_scales_its_fraction_by_length() {
        let parse = super::parse_duration_component_ns;
        let second = 1_000_000_000_u128;

        check!(parse("1", second) == Ok(second));
        check!(parse("2", second) == Ok(2 * second));
        check!(parse("0", second) == Ok(0));

        // The fraction divides by ten raised to its own length.
        check!(
            parse("1.5", second) == Ok(1_500_000_000),
            "one digit is tenths"
        );
        check!(
            parse("1.05", second) == Ok(1_050_000_000),
            "two digits are hundredths"
        );
        check!(
            parse("1.50", second) == Ok(1_500_000_000),
            "a trailing zero changes nothing"
        );
        check!(
            parse(".5", second) == Ok(500_000_000),
            "no whole part is still a number"
        );
        check!(parse("1.", second) == Ok(second), "no fraction either");

        // The multiplier is applied to both halves.
        check!(
            parse("1.5", 1_000) == Ok(1_500),
            "a microsecond scales the same way"
        );

        // What is not a number.
        check!(parse(".", second).is_err(), "a bare point has no digits");
        check!(parse("", second).is_err());
        // Two points is an error, and the same error either way: the split
        // takes the first point, so the second lands in the fraction and fails
        // to parse with the message the explicit check would have given.
        check!(
            parse("1.2.3", second) == Err(r#"invalid number "1.2.3""#.to_string()),
            "named, not merely refused"
        );
        check!(parse("a", second).is_err());
        check!(parse("-1", second).is_err(), "a component is unsigned");

        // A value too large to scale is refused rather than wrapping.
        check!(
            parse(&u128::MAX.to_string(), second).is_err(),
            "out of range"
        );
    }

    /// `parse_logfmt_value` returns the value and how many bytes it consumed.
    /// The length is what the caller resumes from, so every case checks it as
    /// well as the text -- a quoted value's length has to cover both quotes,
    /// which the value itself cannot show.
    #[test]
    fn a_logfmt_value_reports_what_it_consumed() {
        let parse = super::parse_logfmt_value;

        // Bare values run to the first whitespace.
        check!(parse("abc") == Some(("abc".to_string(), 3)));
        check!(
            parse("abc def") == Some(("abc".to_string(), 3)),
            "stops at the space"
        );
        check!(
            parse("") == Some((String::new(), 0)),
            "an empty value consumes nothing"
        );

        // Quoted values consume their quotes: two more than the text.
        check!(parse(r#""abc""#) == Some(("abc".to_string(), 5)));
        check!(
            parse(r#""abc" rest"#) == Some(("abc".to_string(), 5)),
            "and stop at the close"
        );
        check!(
            parse(r#""a b""#) == Some(("a b".to_string(), 5)),
            "whitespace inside quotes"
        );
        check!(
            parse(r#""""#) == Some((String::new(), 2)),
            "an empty quoted value is two bytes"
        );

        // Escapes: the three named ones become control characters, and anything
        // else after a backslash is itself.
        check!(
            parse(r#""a\nb""#) == Some(("a\nb".to_string(), 6)),
            "backslash-n is a newline"
        );
        check!(
            parse(r#""a\tb""#) == Some(("a\tb".to_string(), 6)),
            "backslash-t is a tab"
        );
        check!(
            parse(r#""a\rb""#) == Some(("a\rb".to_string(), 6)),
            "backslash-r is a return"
        );
        check!(
            parse(r#""a\"b""#) == Some((r#"a"b"#.to_string(), 6)),
            "an escaped quote"
        );
        check!(
            parse(r#""a\\b""#) == Some((r"a\b".to_string(), 6)),
            "an escaped backslash"
        );
        check!(
            parse(r#""a\qb""#) == Some(("aqb".to_string(), 6)),
            "an unknown escape is itself"
        );

        // An unterminated quote is not a value at all.
        check!(parse(r#""abc"#) == None);
        check!(parse(r#"""#) == None);
    }

    /// Go-style durations concatenate a number and a unit, and several pairs
    /// add up. Each unit is checked against the nanoseconds it stands for,
    /// since a table of multipliers is exactly where one wrong power of ten
    /// hides.
    #[test]
    fn go_durations_sum_their_components() {
        let parse = super::parse_go_duration_ns;

        check!(parse("1ns").unwrap() == 1);
        check!(parse("1us").unwrap() == 1_000);
        check!(
            parse("1µs").unwrap() == 1_000,
            "the micro sign is accepted too"
        );
        check!(parse("1ms").unwrap() == 1_000_000);
        check!(parse("1s").unwrap() == 1_000_000_000);
        check!(parse("1m").unwrap() == 60_000_000_000);
        check!(parse("1h").unwrap() == 3_600_000_000_000);

        // Several components add rather than replace one another.
        check!(parse("1h30m").unwrap() == 5_400_000_000_000);
        check!(parse("1m1s1ms").unwrap() == 61_001_000_000);
        check!(parse("0s").unwrap() == 0);

        // A fractional component scales by its unit.
        check!(parse("1.5s").unwrap() == 1_500_000_000);

        check!(parse("").is_err(), "an empty duration is not zero");
        check!(parse("10").is_err(), "a number with no unit");
        check!(parse("s").is_err(), "a unit with no number");
        check!(parse("1d").is_err(), "days are not a Go duration unit");
        check!(parse("1x").is_err(), "nor is anything else");
    }

    /// Seconds arrive as a decimal string and become whole nanoseconds. The
    /// fraction is padded rather than parsed as written, so "1.5" is half a
    /// second and not five nanoseconds.
    #[test]
    fn decimal_seconds_become_nanoseconds() {
        let parse = super::parse_seconds_to_ns;

        check!(parse("0").unwrap() == 0);
        check!(parse("1").unwrap() == 1_000_000_000);
        check!(
            parse("1.5").unwrap() == 1_500_000_000,
            "the fraction is padded, not read raw"
        );
        check!(
            parse("0.000000001").unwrap() == 1,
            "nine places is the smallest step"
        );
        check!(parse("1.000000001").unwrap() == 1_000_000_001);
        check!(
            parse("-1.5").unwrap() == -1_500_000_000,
            "the sign applies to the whole value"
        );
        check!(parse("-0").unwrap() == 0);

        check!(parse("").is_none(), "an empty value is not zero");
        check!(parse(".5").is_none(), "the whole part is required");
        check!(
            parse("1.").unwrap() == 1_000_000_000,
            "an empty fraction is none"
        );
        check!(parse("1.0000000001").is_none(), "past nanosecond precision");
        check!(parse("1.2.3").is_none(), "only one point");
        check!(parse("abc").is_none());
        check!(parse("1e9").is_none(), "no exponent form");
    }

    #[test]
    fn step_accepts_seconds_and_go_durations() {
        for (input, want) in [
            // Bare epoch-seconds (what the frontend already accepted).
            ("30", Some(30_000_000_000)),
            // Go-duration forms Grafana's Tempo datasource actually sends.
            ("30s", Some(30_000_000_000)),
            ("5m", Some(300_000_000_000)),
            ("1h", Some(3_600_000_000_000)),
            ("100ms", Some(100_000_000)),
            ("1m30s", Some(90_000_000_000)),
            // Garbage is still rejected.
            ("nonsense", None),
            ("30q", None),
        ] {
            check!(parse_step_to_ns(input) == want);
        }
    }

    #[test]
    fn tags_to_traceql_rejects_keys_with_metacharacters() {
        // Benign keys convert to a properly-quoted attribute match.
        assert2::assert!(tags_to_traceql("svc=b") == Some("{ .svc = \"b\" }".to_string()));
        assert2::assert!(
            tags_to_traceql("span:name=op") == Some("{ span:name = \"op\" }".to_string())
        );
        // A key carrying TraceQL-significant characters injects structure when
        // interpolated unquoted, so it is rejected.
        assert2::assert!(tags_to_traceql("a}=c").is_none());
        assert2::assert!(tags_to_traceql("a\"b=c").is_none());
        // The value side stays safely quoted even with metacharacters.
        assert2::assert!(
            tags_to_traceql("svc=a\"}||x") == Some("{ .svc = \"a\\\"}||x\" }".to_string())
        );
    }
}

mod backend_error_response;
mod bounded_count;
mod echo;
mod exemplar_limit;
mod key_is_safe_attribute;
mod metrics_query_param;
mod optional_seconds;
mod optional_time_bounds;
mod parse_duration_component_ns;
mod parse_go_duration_ns;
mod parse_logfmt_tags;
mod parse_logfmt_value;
mod parse_scope;
mod parse_seconds_to_ns;
mod parse_step_to_ns;
mod query_instant;
mod query_param;
mod query_range;
mod ready;
mod required_seconds;
mod required_step;
mod required_time_bounds;
mod router_with_backend;
mod scope_name;
mod scope_param;
mod search;
mod search_query;
mod search_tag_values_v2;
mod search_tags_v2;
mod tags_to_traceql;
mod tenant;
mod tenant_header;
mod trace_by_id;

use backend_error_response::backend_error_response;
use bounded_count::bounded_count;
use echo::echo;
use exemplar_limit::exemplar_limit;
use key_is_safe_attribute::key_is_safe_attribute;
use metrics_query_param::metrics_query_param;
use optional_seconds::optional_seconds;
use optional_time_bounds::optional_time_bounds;
use parse_duration_component_ns::parse_duration_component_ns;
use parse_go_duration_ns::parse_go_duration_ns;
use parse_logfmt_tags::parse_logfmt_tags;
use parse_logfmt_value::parse_logfmt_value;
use parse_scope::parse_scope;
use parse_seconds_to_ns::parse_seconds_to_ns;
use parse_step_to_ns::parse_step_to_ns;
use query_instant::query_instant;
use query_param::query_param;
use query_range::query_range;
use ready::ready;
use required_seconds::required_seconds;
use required_step::required_step;
use required_time_bounds::required_time_bounds;
pub use router_with_backend::router_with_backend;
use scope_name::scope_name;
use scope_param::scope_param;
use search::search;
use search_query::search_query;
use search_tag_values_v2::search_tag_values_v2;
use search_tags_v2::search_tags_v2;
use tags_to_traceql::tags_to_traceql;
use tenant::tenant;
use tenant_header::TENANT_HEADER;
use trace_by_id::trace_by_id;
