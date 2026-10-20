//! A proptest generator of near-miss queries, shared by the `LogQL`,
//! `TraceQL` and `PromQL` parser property suites and the Pyroscope `/ingest`
//! query-string suite.
//!
//! `//crates/traceql`, `//crates/promql` and `//crates/profiles` reach this
//! file with `#[path]`, so it depends only on `proptest`.

use proptest::prelude::*;

struct CharBounds {
    start: usize,
    end: usize,
}

#[derive(Debug)]
struct SeedSpliceCase {
    base: &'static str,
    donor: &'static str,
    cut_a: prop::sample::Index,
    cut_b: prop::sample::Index,
    paste_a: prop::sample::Index,
    paste_b: prop::sample::Index,
}

// A valid seed query with one splice applied: a range of it is replaced by a
// slice of another seed, or by nothing. Near-miss inputs like these reach
// error paths that neither pure noise nor a token salad finds.
pub fn mutated_seed(seeds: &'static [&'static str]) -> impl Strategy<Value = String> {
    prop::sample::select(seeds)
        .prop_flat_map(move |base| {
            prop::sample::select(seeds).prop_flat_map(move |donor| {
                any::<prop::sample::Index>().prop_flat_map(move |cut_a| {
                    any::<prop::sample::Index>().prop_flat_map(move |cut_b| {
                        any::<prop::sample::Index>().prop_flat_map(move |paste_a| {
                            any::<prop::sample::Index>().prop_map(move |paste_b| SeedSpliceCase {
                                base,
                                donor,
                                cut_a,
                                cut_b,
                                paste_a,
                                paste_b,
                            })
                        })
                    })
                })
            })
        })
        .prop_map(|case| {
            let base = case.base;
            let donor = case.donor;
            let cut = ordered_char_bounds(base, case.cut_a, case.cut_b);
            let paste = ordered_char_bounds(donor, case.paste_a, case.paste_b);
            let mut out = String::new();
            out.push_str(&base[..cut.start]);
            out.push_str(&donor[paste.start..paste.end]);
            out.push_str(&base[cut.end..]);
            out
        })
}

// Two ascending char boundaries into `text`, so slicing never splits a
// multi-byte character and never panics inside the generator itself.
fn ordered_char_bounds(
    text: &str,
    first: prop::sample::Index,
    second: prop::sample::Index,
) -> CharBounds {
    let bounds: Vec<usize> = (0..=text.len())
        .filter(|at| text.is_char_boundary(*at))
        .collect();
    let a = *first.get(&bounds);
    let b = *second.get(&bounds);
    CharBounds {
        start: a.min(b),
        end: a.max(b),
    }
}
