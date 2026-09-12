//! Property tests for the Pyroscope `/ingest` query-string parser.
//!
//! `/ingest` is an unauthenticated door. Its query string carries the
//! application name and its label set in Pyroscope's `app{k=v,k2=v2}` syntax,
//! plus the format, sample rate, units and time bounds, and
//! [`parse_ingest_query`] is the only thing between that string and the
//! ingest path. Two properties hold it.
//!
//! `parse_ingest_query_never_panics` is the totality property the code style
//! guide requires of a decoder at an untrusted edge: any string at all is
//! answered with an [`IngestQuery`] or a `ProfilesError`.
//!
//! `rendering_an_ingest_query_round_trips` builds a query, renders it as the
//! query string a Pyroscope client would send, and asserts the parse recovers
//! the whole struct. The renderer here is independent of the parser, which is
//! what makes the property worth running.

use std::fmt::Write as _;

use assert2::assert;
use krabka_profiles::ingest::{IngestFormat, IngestQuery, parse_ingest_query};
use proptest::prelude::*;

/// Valid query strings, used as the seeds the mutation strategy edits.
const SEED_QUERIES: &[&str] = &[
    "name=app",
    "name=app%7Bk%3Dv%7D",
    "name=app{k=v}",
    "name=app{k=v,k2=v2}",
    "name=app&format=pprof",
    "name=app&format=jfr&sampleRate=97",
    "name=app&units=bytes",
    "name=app&from=1700000000000&until=1700000060000",
    "name=app&from=1700000000&until=1700000060",
    "name=app&spyName=gospy&format=trie&sampleRate=100&units=samples",
];

/// The pieces the query string is built from: parameter names, separators,
/// escapes and the label-set punctuation.
const TOKENS: &[&str] = &[
    "&",
    "=",
    "?",
    "%",
    "+",
    "{",
    "}",
    ",",
    "\"",
    " ",
    "name",
    "format",
    "sampleRate",
    "units",
    "from",
    "until",
    "spyName",
    "app",
    "pprof",
    "jfr",
    "trie",
    "tree",
    "lines",
    "speedscope",
    "groups",
    "0",
    "-1",
    "100",
    "99999999999999999999",
    "%7B",
    "%7D",
    "%2",
    "%zz",
    "k",
    "v",
];

fn arbitrary_text() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            2 => any::<char>(),
            3 => prop::char::range('\u{0}', '\u{7f}'),
            1 => Just('%'),
            1 => Just('é'),
            1 => Just('\u{1f600}'),
        ],
        0..48,
    )
    .prop_map(String::from_iter)
}

fn token_salad() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(TOKENS), 1..16).prop_map(|parts| parts.concat())
}

/// A valid seed with one splice applied: a range of it is replaced by a slice
/// of another seed, or by nothing.
fn mutated_seed() -> impl Strategy<Value = String> {
    (
        prop::sample::select(SEED_QUERIES),
        prop::sample::select(SEED_QUERIES),
        any::<prop::sample::Index>(),
        any::<prop::sample::Index>(),
        any::<prop::sample::Index>(),
        any::<prop::sample::Index>(),
    )
        .prop_map(|(base, donor, cut_a, cut_b, paste_a, paste_b)| {
            let cut = ordered_char_bounds(base, cut_a, cut_b);
            let paste = ordered_char_bounds(donor, paste_a, paste_b);
            let mut out = String::new();
            out.push_str(&base[..cut.0]);
            out.push_str(&donor[paste.0..paste.1]);
            out.push_str(&base[cut.1..]);
            out
        })
}

/// Two ascending char boundaries into `text`, so the generator's own slicing
/// never splits a multi-byte character.
fn ordered_char_bounds(
    text: &str,
    first: prop::sample::Index,
    second: prop::sample::Index,
) -> (usize, usize) {
    let bounds: Vec<usize> = (0..=text.len())
        .filter(|at| text.is_char_boundary(*at))
        .collect();
    let a = *first.get(&bounds);
    let b = *second.get(&bounds);
    (a.min(b), a.max(b))
}

fn arbitrary_query() -> impl Strategy<Value = String> {
    prop_oneof![
        2 => arbitrary_text(),
        3 => token_salad(),
        3 => mutated_seed(),
    ]
}

// -- Generators for the round-trip property ------------------------------

/// Application names and label parts. `split_app_labels` trims each part and
/// strips a surrounding pair of quotes, so the alphabet stays clear of
/// whitespace, quotes, and the `{`, `}`, `,` and `=` that delimit the syntax.
const NAMES: &[&str] = &["app", "my.app", "a-b", "svc_1", "APP"];

fn arb_format() -> impl Strategy<Value = IngestFormat> {
    prop::sample::select(&[
        IngestFormat::Pprof,
        IngestFormat::Jfr,
        IngestFormat::Trie,
        IngestFormat::Tree,
        IngestFormat::Lines,
        IngestFormat::Speedscope,
        IngestFormat::Groups,
    ])
}

/// A whole ingest query.
///
/// The time bounds stay above ten billion, where `parse_unix_time_ms` reads
/// the number as milliseconds and hands it back unchanged. Below that it reads
/// seconds and multiplies by a thousand, which is a conversion rather than a
/// round trip, and `tests/pyroscope_differential.rs` covers it by example.
fn arb_ingest_query() -> impl Strategy<Value = IngestQuery> {
    (
        prop::sample::select(NAMES),
        prop::collection::vec(
            (prop::sample::select(NAMES), prop::sample::select(NAMES)),
            0..4,
        ),
        arb_format(),
        1_u32..1_000_000,
        prop::sample::select(&["count", "bytes", "samples", "nanoseconds"]),
        prop::option::of(10_000_000_000_i64..2_000_000_000_000),
        prop::option::of(10_000_000_000_i64..2_000_000_000_000),
        prop::sample::select(&["unknown", "gospy", "pyspy", "rbspy"]),
        prop::sample::select(&["wall", "cpu", "alloc"]),
    )
        .prop_map(
            |(name, labels, format, sample_rate, units, from_ms, until_ms, spy_name, jfr_event)| {
                IngestQuery {
                    name: name.to_owned(),
                    profile_type_suffix: None,
                    labels: labels
                        .into_iter()
                        .map(|(key, value)| (key.to_owned(), value.to_owned()))
                        .collect(),
                    format,
                    sample_rate,
                    units: units.to_owned(),
                    from_ms,
                    until_ms,
                    spy_name: spy_name.to_owned(),
                    jfr_event: jfr_event.to_owned(),
                }
            },
        )
}

// -- The renderer --------------------------------------------------------

fn format_text(format: IngestFormat) -> &'static str {
    match format {
        IngestFormat::Pprof => "pprof",
        IngestFormat::Jfr => "jfr",
        IngestFormat::Trie => "trie",
        IngestFormat::Tree => "tree",
        IngestFormat::Lines => "lines",
        IngestFormat::Speedscope => "speedscope",
        // Anything the parser does not recognise falls back to `Groups`, and
        // `groups` is the spelling a Pyroscope client sends for it.
        IngestFormat::Groups => "groups",
    }
}

/// Percent-encodes everything outside the unreserved set, the way a client
/// building the `name` parameter does.
fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

fn render_ingest_query(query: &IngestQuery) -> String {
    let mut name = query.name.clone();
    if !query.labels.is_empty() {
        let pairs: Vec<String> = query
            .labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        name.push('{');
        name.push_str(&pairs.join(","));
        name.push('}');
    }

    let mut parts = vec![
        format!("name={}", urlencode(&name)),
        format!("format={}", format_text(query.format)),
        format!("sampleRate={}", query.sample_rate),
        format!("units={}", urlencode(&query.units)),
    ];
    if let Some(from_ms) = query.from_ms {
        parts.push(format!("from={from_ms}"));
    }
    if let Some(until_ms) = query.until_ms {
        parts.push(format!("until={until_ms}"));
    }
    parts.push(format!("spyName={}", urlencode(&query.spy_name)));
    parts.push(format!("event={}", urlencode(&query.jfr_event)));
    parts.join("&")
}

/// The seeds carry the mutation strategy, so a typo in one would quietly
/// narrow what the totality property covers.
#[test]
fn the_seed_queries_are_valid_ingest_queries() {
    for query in SEED_QUERIES {
        assert!(parse_ingest_query(query).is_ok(), "seed query: {query}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// No string panics the ingest query parser.
    #[test]
    fn parse_ingest_query_never_panics(query in arbitrary_query()) {
        let _ = parse_ingest_query(&query);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Rendering an ingest query and parsing it back yields the same query.
    #[test]
    fn rendering_an_ingest_query_round_trips(query in arb_ingest_query()) {
        let rendered = render_ingest_query(&query);
        let reparsed = parse_ingest_query(&rendered)
            .map_err(|error| TestCaseError::fail(format!("{rendered} does not parse: {error}")))?;

        prop_assert_eq!(reparsed, query);
    }
}
