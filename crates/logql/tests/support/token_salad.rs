//! A proptest generator of grammar-token salads, shared by the `LogQL`,
//! `TraceQL` and `PromQL` parser property suites.
//!
//! `//crates/traceql` and `//crates/promql` reach this file with `#[path]`, so
//! it depends only on `proptest`.

use proptest::prelude::*;

// Concatenated grammar tokens, each followed by nothing or a space.
pub fn spaced_token_salad(tokens: &'static [&'static str]) -> impl Strategy<Value = String> {
    prop::collection::vec((prop::sample::select(tokens), any::<bool>()), 1..14).prop_map(|parts| {
        let mut out = String::new();
        for (token, spaced) in parts {
            out.push_str(token);
            if spaced {
                out.push(' ');
            }
        }
        out
    })
}
