//! What a push-then-render suite reads and writes: the gzip of a pushed
//! profile, and the frame names and ticks of the flamebearer that comes back.
//!
//! Shared by this binary's suites and, through `#[path]`, by suites under
//! `tests/`, so it names only external crates.

use std::io::Write as _;

use flate2::{Compression, write::GzEncoder};
use serde_json::Value;

/// The rendered flamebearer's frame names, sorted, without `total`.
pub fn flame_names(value: &Value) -> Vec<String> {
    let mut names: Vec<String> = value
        .pointer("/flamebearer/names")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|names| names.iter())
        .filter_map(Value::as_str)
        .filter(|name| *name != "total" && !name.is_empty())
        .map(ToString::to_string)
        .collect();
    names.sort();
    names
}

/// The rendered flamebearer's total ticks.
pub fn flame_ticks(value: &Value) -> Option<i64> {
    value
        .pointer("/flamebearer/numTicks")
        .or_else(|| value.pointer("/flamebearer/total"))
        .and_then(Value::as_i64)
}

/// What a push-then-render suite expects back from the render.
pub struct ExpectedFlame<'a> {
    /// The frame names, sorted, without `total`.
    pub names: &'a [&'a str],
    /// The total ticks.
    pub ticks: i64,
}

/// Checks a rendered flamebearer against `expected`, and that its metadata
/// carries the CPU profile type's unit.
pub fn check_rendered_flame(render: &Value, expected: &ExpectedFlame<'_>) {
    assert2::check!(flame_names(render) == expected.names);
    assert2::check!(flame_ticks(render) == Some(expected.ticks));
    assert2::check!(
        render.pointer("/metadata/units").and_then(Value::as_str) == Some("nanoseconds"),
        "render metadata must carry the profile type's unit, got {render}"
    );
}

pub fn gzip_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).expect("gzip write");
    encoder.finish().expect("gzip finish")
}
