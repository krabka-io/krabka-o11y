//! A proptest generator of grammar-token salads, shared by the `LogQL`,
//! `TraceQL` and `PromQL` parser property suites.
//!
//! `//crates/traceql` and `//crates/promql` reach this file with `#[path]`, so
//! it depends only on `proptest`.

use proptest::prelude::*;

#[derive(Debug)]
struct SpacedToken {
    token: &'static str,
    spaced: bool,
}

// Concatenated grammar tokens, each followed by nothing or a space.
pub fn spaced_token_salad(tokens: &'static [&'static str]) -> impl Strategy<Value = String> {
    let token = prop::sample::select(tokens)
        .prop_flat_map(|token| any::<bool>().prop_map(move |spaced| SpacedToken { token, spaced }));
    prop::collection::vec(token, 1..14).prop_map(|parts| {
        let mut out = String::new();
        for part in parts {
            let token = part.token;
            out.push_str(token);
            if part.spaced {
                out.push(' ');
            }
        }
        out
    })
}
