//! Pure label-rewrite and ordering transforms for the `label_replace`,
//! `label_join`, `sort`, and `sort_desc` operator paths.
//!
//! These four functions do not lower to a `DataFusion` projection over a leaf
//! table, unlike the selector, rate, `*_over_time`, and scalar-math paths. They
//! work on the already-assembled inner instant vector, which holds one
//! [`InstantSample`] per matched series. `label_replace` and `label_join`
//! rewrite its label columns, and `sort` and `sort_desc` reorder its rows.
//!
//! Each transform is a pure function over the assembled vector. The engine
//! therefore recurses into the inner expression and assembles it with that
//! shape's own NaN and staleness semantics. The engine then applies one of these
//! transforms at assembly time, and does not re-emit through the operator chain.
//!
//! The same functions back the interpreter's `eval_label_replace_call`,
//! `eval_label_join_call`, and `eval_sort_call`, so the operator path matches
//! the interpreter by construction. The match covers `$1` and `${name}`
//! capture-group expansion, empty-replacement label writes, no-match
//! passthrough, the `separator`-join semantics, and the value ordering with the
//! `labels_key` tiebreak. That ordering places `NaN` last for `sort` and for
//! `sort_desc`, as Prometheus does.

use std::cmp::Ordering;

use krabka_blockstore::Labels;
use regex::Regex;

use crate::{
    PromqlError,
    error::Result,
    result::{InstantSample, SampleValue},
};

#[cfg(test)]
mod tests {

    use super::*;

    fn sample(pairs: &[(&str, &str)], value: f64) -> InstantSample {
        let mut labels = Labels::new();
        for (name, val) in pairs {
            labels.insert(*name, *val);
        }
        InstantSample {
            labels,
            ts_ms: 1_000,
            value: SampleValue::Float(value),
        }
    }

    #[test]
    fn label_replace_capture_group_expands() {
        let out = apply_label_replace(
            vec![sample(&[("__name__", "m"), ("src", "a-b")], 1.0)],
            "dst",
            "$1",
            "src",
            "(.*)-.*",
        )
        .unwrap();
        // `dst` gets the capture-group expansion; `__name__` is preserved
        // (label_replace does not drop it).
        assert2::assert!(
            out == vec![sample(
                &[("__name__", "m"), ("src", "a-b"), ("dst", "a")],
                1.0
            )]
        );
    }

    #[test]
    fn label_replace_no_match_passthrough() {
        let input = vec![sample(&[("__name__", "m"), ("src", "zzz")], 1.0)];
        let out = apply_label_replace(input.clone(), "dst", "$1", "src", "(\\d+)").unwrap();
        // No match: series unchanged, no `dst` label added.
        assert2::assert!(out == input);
    }

    #[test]
    fn label_replace_empty_replacement_removes_the_label() {
        let out = apply_label_replace(
            vec![sample(
                &[("__name__", "m"), ("src", "abc"), ("dst", "old")],
                1.0,
            )],
            "dst",
            "",
            "src",
            ".*",
        )
        .unwrap();
        // Prometheus writes through `labels.Builder.Set`, which deletes a label
        // set to the empty string rather than store it empty.
        assert2::assert!(out == vec![sample(&[("__name__", "m"), ("src", "abc")], 1.0)]);
    }

    #[test]
    fn label_join_empty_join_removes_the_label() {
        let out = apply_label_join(
            vec![sample(&[("a", "1"), ("dst", "old")], 1.0)],
            "dst",
            "",
            &["missing".to_string(), "alsomissing".to_string()],
        );
        assert2::assert!(out == vec![sample(&[("a", "1")], 1.0)]);
    }

    #[test]
    fn label_replace_anchors_regex_fully() {
        // Prometheus fully anchors the regex (`^(?:foo)$`), so a `foo` pattern
        // must NOT match the substring inside `xfooy`. A raw unanchored `Regex`
        // would wrongly match and write `dst`.
        let input = vec![sample(&[("__name__", "m"), ("src", "xfooy")], 1.0)];
        let out = apply_label_replace(input.clone(), "dst", "$0", "src", "foo").unwrap();
        assert2::assert!(out == input);
        assert2::assert!(out[0].labels.get("dst").is_none());

        // The same pattern matches when it spans the entire value.
        let out = apply_label_replace(
            vec![sample(&[("__name__", "m"), ("src", "foo")], 1.0)],
            "dst",
            "$0",
            "src",
            "foo",
        )
        .unwrap();
        assert2::assert!(out[0].labels.get("dst") == Some("foo"));
    }

    #[test]
    fn label_replace_invalid_regex_errors() {
        let err = apply_label_replace(vec![sample(&[("src", "x")], 1.0)], "dst", "$1", "src", "(")
            .unwrap_err();
        assert2::assert!(matches!(err, PromqlError::Plan(_)));
    }

    #[test]
    fn label_join_joins_sources_with_separator() {
        let out = apply_label_join(
            vec![sample(&[("__name__", "m"), ("a", "1"), ("b", "2")], 1.0)],
            "dst",
            "-",
            &["a".to_string(), "b".to_string()],
        );
        assert2::assert!(out[0].labels.get("dst") == Some("1-2"));
    }

    #[test]
    fn label_join_missing_source_is_empty() {
        let out = apply_label_join(
            vec![sample(&[("a", "1")], 1.0)],
            "dst",
            ",",
            &["a".to_string(), "missing".to_string()],
        );
        assert2::assert!(out[0].labels.get("dst") == Some("1,"));
    }

    /// The series whose `l` label spells the post-sort order, by label.
    fn order(out: &[InstantSample]) -> Vec<&str> {
        out.iter()
            .map(|s| s.labels.get("l").unwrap_or(""))
            .collect()
    }

    #[test]
    fn sort_orders_by_value_and_places_nan_last_in_both_directions() {
        for (direction, want) in [
            (SortOrder::Ascending, vec!["a", "b", "n"]),
            (SortOrder::Descending, vec!["b", "a", "n"]),
        ] {
            let out = apply_sort(
                vec![
                    sample(&[("l", "b")], 2.0),
                    sample(&[("l", "n")], f64::NAN),
                    sample(&[("l", "a")], 1.0),
                ],
                direction,
            );
            assert2::assert!(order(&out) == want);
            assert2::assert!(matches!(out[2].value, SampleValue::Float(v) if v.is_nan()));
        }
    }

    #[test]
    fn sort_breaks_ties_by_label_key() {
        let out = apply_sort(
            vec![sample(&[("l", "z")], 1.0), sample(&[("l", "a")], 1.0)],
            SortOrder::Ascending,
        );
        assert2::assert!(out[0].labels.get("l") == Some("a"));
        assert2::assert!(out[1].labels.get("l") == Some("z"));
    }
}

mod apply_label_join;
mod apply_label_replace;
mod apply_sort;
mod apply_sort_by_label;
mod compare_label_values;
mod labels_key;
mod natural_chunks;
mod natural_less;
mod set_label_value;
mod sort_order;
mod sort_value;

pub use apply_label_join::apply_label_join;
pub use apply_label_replace::apply_label_replace;
pub use apply_sort::apply_sort;
pub use apply_sort_by_label::apply_sort_by_label;
use compare_label_values::compare_label_values;
use labels_key::labels_key;
use natural_chunks::natural_chunks;
use natural_less::natural_less;
use set_label_value::set_label_value;
pub use sort_order::SortOrder;
use sort_value::sort_value;
