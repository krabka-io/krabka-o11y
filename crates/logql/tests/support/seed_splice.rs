//! A proptest generator of near-miss queries, shared by the `LogQL` and
//! `TraceQL` parser property suites.
//!
//! `//crates/traceql` reaches this file with `#[path]`, so it depends only on
//! `proptest`.

use proptest::prelude::*;

// A valid seed query with one splice applied: a range of it is replaced by a
// slice of another seed, or by nothing. Near-miss inputs like these reach
// error paths that neither pure noise nor a token salad finds.
pub fn mutated_seed(seeds: &'static [&'static str]) -> impl Strategy<Value = String> {
    (
        prop::sample::select(seeds),
        prop::sample::select(seeds),
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

// Two ascending char boundaries into `text`, so slicing never splits a
// multi-byte character and never panics inside the generator itself.
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
