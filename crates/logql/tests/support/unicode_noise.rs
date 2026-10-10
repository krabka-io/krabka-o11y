//! A proptest generator of arbitrary Unicode text, shared by the `LogQL` and
//! `PromQL` parser property suites.
//!
//! `//crates/promql` reaches this file with `#[path]`, so it depends only on
//! `proptest`.

use proptest::prelude::*;

/// Arbitrary Unicode, weighted towards the code points that trip a
/// byte-indexing parser: control characters, multi-byte characters, and the
/// top of the code point range.
pub fn arbitrary_text() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            2 => any::<char>(),
            3 => prop::char::range('\u{0}', '\u{7f}'),
            1 => Just('\u{10ffff}'),
            1 => Just('é'),
            1 => Just('\u{1f600}'),
        ],
        0..48,
    )
    .prop_map(String::from_iter)
}
