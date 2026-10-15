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

use regex::Regex;

use crate::{
    PromqlError, PromqlLabels as Labels, PromqlString,
    error::Result,
    result::{InstantSample, SampleValue},
};

#[cfg(test)]
mod tests {

    use super::*;

    struct LabelPair {
        name: &'static str,
        value: &'static str,
    }

    impl LabelPair {
        const fn new(name: &'static str, value: &'static str) -> Self {
            Self { name, value }
        }
    }

    fn sample(pairs: &[LabelPair], value: f64) -> InstantSample {
        let mut labels = Labels::new();
        for pair in pairs {
            labels.insert(pair.name, pair.value);
        }
        InstantSample {
            labels,
            ts_ms: 1_000,
            value: SampleValue::Float(value),
            drop_name: false,
        }
    }

    #[test]
    fn label_replace_capture_group_expands() {
        let out = apply_label_replace(
            vec![sample(
                &[
                    LabelPair::new("__name__", "m"),
                    LabelPair::new("src", "a-b"),
                ],
                1.0,
            )],
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
                &[
                    LabelPair::new("__name__", "m"),
                    LabelPair::new("src", "a-b"),
                    LabelPair::new("dst", "a")
                ],
                1.0
            )]
        );
    }

    #[test]
    fn label_replace_no_match_passthrough() {
        let input = vec![sample(
            &[
                LabelPair::new("__name__", "m"),
                LabelPair::new("src", "zzz"),
            ],
            1.0,
        )];
        let out = apply_label_replace(input.clone(), "dst", "$1", "src", "(\\d+)").unwrap();
        // No match: series unchanged, no `dst` label added.
        assert2::assert!(out == input);
    }

    #[test]
    fn label_replace_empty_replacement_removes_the_label() {
        let out = apply_label_replace(
            vec![sample(
                &[
                    LabelPair::new("__name__", "m"),
                    LabelPair::new("src", "abc"),
                    LabelPair::new("dst", "old"),
                ],
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
        assert2::assert!(
            out == vec![sample(
                &[
                    LabelPair::new("__name__", "m"),
                    LabelPair::new("src", "abc")
                ],
                1.0
            )]
        );
    }

    #[test]
    fn label_join_empty_join_removes_the_label() {
        let out = apply_label_join(
            vec![sample(
                &[LabelPair::new("a", "1"), LabelPair::new("dst", "old")],
                1.0,
            )],
            "dst",
            "",
            &["missing".to_string(), "alsomissing".to_string()],
        );
        assert2::assert!(out == vec![sample(&[LabelPair::new("a", "1")], 1.0)]);
    }

    #[test]
    fn label_replace_anchors_regex_fully() {
        // Prometheus fully anchors the regex (`^(?:foo)$`), so a `foo` pattern
        // must NOT match the substring inside `xfooy`. A raw unanchored `Regex`
        // would wrongly match and write `dst`.
        let input = vec![sample(
            &[
                LabelPair::new("__name__", "m"),
                LabelPair::new("src", "xfooy"),
            ],
            1.0,
        )];
        let out = apply_label_replace(input.clone(), "dst", "$0", "src", "foo").unwrap();
        assert2::assert!(out == input);
        assert2::assert!(out[0].labels.get("dst").is_none());

        // The same pattern matches when it spans the entire value.
        let out = apply_label_replace(
            vec![sample(
                &[
                    LabelPair::new("__name__", "m"),
                    LabelPair::new("src", "foo"),
                ],
                1.0,
            )],
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
        let err = apply_label_replace(
            vec![sample(&[LabelPair::new("src", "x")], 1.0)],
            "dst",
            "$1",
            "src",
            "(",
        )
        .unwrap_err();
        assert2::assert!(matches!(err, PromqlError::Exec(_)));
    }

    #[test]
    fn label_join_joins_sources_with_separator() {
        let out = apply_label_join(
            vec![sample(
                &[
                    LabelPair::new("__name__", "m"),
                    LabelPair::new("a", "1"),
                    LabelPair::new("b", "2"),
                ],
                1.0,
            )],
            "dst",
            "-",
            &["a".to_string(), "b".to_string()],
        );
        assert2::assert!(out[0].labels.get("dst") == Some("1-2"));
    }

    #[test]
    fn label_join_missing_source_is_empty() {
        let out = apply_label_join(
            vec![sample(&[LabelPair::new("a", "1")], 1.0)],
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

    struct SortExpectation {
        direction: SortOrder,
        expected: Vec<&'static str>,
    }

    #[test]
    fn sort_orders_by_value_and_places_nan_last_in_both_directions() {
        for expectation in [
            SortExpectation {
                direction: SortOrder::Ascending,
                expected: vec!["a", "b", "n"],
            },
            SortExpectation {
                direction: SortOrder::Descending,
                expected: vec!["b", "a", "n"],
            },
        ] {
            let out = apply_sort(
                vec![
                    sample(&[LabelPair::new("l", "b")], 2.0),
                    sample(&[LabelPair::new("l", "n")], f64::NAN),
                    sample(&[LabelPair::new("l", "a")], 1.0),
                ],
                expectation.direction,
            );
            assert2::assert!(order(&out) == expectation.expected);
            assert2::assert!(matches!(out[2].value, SampleValue::Float(v) if v.is_nan()));
        }
    }

    #[test]
    fn sort_breaks_ties_by_label_key() {
        let out = apply_sort(
            vec![
                sample(&[LabelPair::new("l", "z")], 1.0),
                sample(&[LabelPair::new("l", "a")], 1.0),
            ],
            SortOrder::Ascending,
        );
        assert2::assert!(out[0].labels.get("l") == Some("a"));
        assert2::assert!(out[1].labels.get("l") == Some("z"));
    }
}

/// Returns the canonical `name=value\n…` rendering of a label set.
///
/// This rendering is the sort tiebreak and the collision key. It matches the
/// interpreter's `labels_key`.
pub(crate) fn labels_key(labels: &Labels) -> String {
    labels.order_key()
}

/// Returns the float value of a sample, or `NaN` for a histogram sample.
///
/// This matches the interpreter's `float_sample_value(...).unwrap_or(f64::NAN)`
/// in the sort comparator.
pub(crate) fn sort_value(sample: &InstantSample) -> f64 {
    match sample.value {
        SampleValue::Float(value) => value,
        SampleValue::Histogram(_) => f64::NAN,
    }
}

/// Applies `label_join(v, dst_label, separator, src_label_1, …)` to an
/// already-assembled instant vector.
///
/// For every series, this function sets `dst_label` to the `separator`-joined
/// values of the listed source labels. A missing label contributes the empty
/// string, and a join that comes out empty REMOVES `dst_label`, because
/// Prometheus writes the result through `labels.Builder.Set`, which deletes a
/// label rather than store it empty.
#[must_use]
pub fn apply_label_join(
    samples: Vec<InstantSample>,
    dst_label: &str,
    separator: &str,
    src_labels: &[String],
) -> Vec<InstantSample> {
    let source_names = src_labels.iter().map(String::as_str).collect::<Vec<_>>();
    apply_join_samples(samples, dst_label, separator.as_bytes(), &source_names)
}

/// Sorts an already-assembled instant vector by sample value in `order`.
///
/// Histogram samples are dropped, not sorted: `funcSort` and `funcSortDesc`
/// pass the vector through `filterFloats` first, so a histogram series never
/// reaches the output. Ties break by canonical label key.
#[must_use]
pub fn apply_sort(samples: Vec<InstantSample>, order: SortOrder) -> Vec<InstantSample> {
    let mut samples: Vec<InstantSample> = samples
        .into_iter()
        .filter(|sample| matches!(sample.value, SampleValue::Float(_)))
        .collect();
    samples.sort_by(|left, right| {
        order
            .compare(sort_value(left), sort_value(right))
            .then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
    });
    samples
}

/// Sorts an already-assembled instant vector by the values of the named labels
/// in `order`.
///
/// The sort is over the listed labels first, in the given order, and then over
/// the full canonical label key. The tiebreak follows `order` too:
/// `funcSortByLabelDesc` ends on `-labels.Compare(a, b)`, so two series that
/// agree on every listed label come back in DESCENDING label order.
#[must_use]
pub fn apply_sort_by_label(
    mut samples: Vec<InstantSample>,
    label_names: &[String],
    order: SortOrder,
) -> Vec<InstantSample> {
    samples.sort_by(|left, right| {
        compare_label_values(&left.labels, &right.labels, label_names, order).then_with(|| {
            let tiebreak = labels_key(&left.labels).cmp(&labels_key(&right.labels));
            match order {
                SortOrder::Ascending => tiebreak,
                SortOrder::Descending => tiebreak.reverse(),
            }
        })
    });
    samples
}

/// Splits a label value into the runs of digits and runs of non-digits that
/// natural order compares.
///
/// This is `natsort`'s `chunkify`, whose `(\d+|\D+)` is ASCII-only in RE2, so a
/// non-ASCII digit stays in a non-digit run.
pub(crate) fn natural_chunks(value: &[u8]) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    let mut rest = value;
    while !rest.is_empty() {
        let digits = rest[0].is_ascii_digit();
        let end = rest
            .iter()
            .position(|byte| byte.is_ascii_digit() != digits)
            .unwrap_or(rest.len());
        let split = rest.split_at(end);
        chunks.push(split.0);
        rest = split.1;
    }
    chunks
}

/// Writes `value` to `name`, or removes `name` when `value` is empty.
///
/// `label_replace` and `label_join` both write their result through
/// `labels.Builder.Set`, and that method deletes the label when the value is
/// empty, because Prometheus holds an empty label and a missing one to be the
/// same thing.
pub(crate) fn set_label_value(
    labels: &Labels,
    name: &str,
    value: impl Into<crate::PromqlString>,
) -> Labels {
    let value = value.into();
    if value.as_bytes().is_empty() {
        return labels
            .iter()
            .filter(|pair| pair.name.as_str() != name)
            .map(|pair| krabka_metrics::MetricLabelPair::new(pair.name.clone(), pair.value.clone()))
            .collect();
    }
    let mut labels = labels.clone();
    labels.insert(name, value);
    labels
}

/// Sort order for the `sort` / `sort_desc` functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl SortOrder {
    /// Compares two sample values in this order, and puts a `NaN` last in both.
    ///
    /// Prometheus sends a `NaN` to the bottom under `sort` and under
    /// `sort_desc`. `funcSort` and `funcSortDesc` each build a heap that ranks
    /// `NaN` first, then sort the reverse of that heap. A `NaN` is neither the
    /// largest value nor the smallest one, so no direction puts it first.
    ///
    /// `total_cmp` alone does not give that result. It ranks a positive `NaN`
    /// above every finite value. Ascending order then sends the `NaN` to the
    /// end, but descending order sends it to the front. So this method decides
    /// the `NaN` cases first, and `total_cmp` compares two numbers only.
    ///
    /// A histogram sample reaches this comparison as a `NaN` through
    /// `sort_value`, and so sorts last as well.
    pub(crate) fn compare(self, left: f64, right: f64) -> Ordering {
        if left.is_nan() {
            if right.is_nan() {
                Ordering::Equal
            } else {
                Ordering::Greater
            }
        } else if right.is_nan() {
            Ordering::Less
        } else {
            match self {
                Self::Ascending => left.total_cmp(&right),
                Self::Descending => right.total_cmp(&left),
            }
        }
    }
}

/// Whether `left` precedes `right` in natural order.
///
/// `sort_by_label` orders label values with `facette/natsort`, so `cpu="2"`
/// sorts before `cpu="10"`: each value splits into runs of digits and runs of
/// non-digits, and a pair of digit runs compares as integers. A digit run too
/// long for an `i64` falls back to a byte comparison, matching `strconv.Atoi`'s
/// overflow error.
///
/// This answers only "does `left` precede `right`", the one question
/// `natsort.Compare` answers, so that the caller can reproduce
/// `funcSortByLabel`'s ordering exactly, quirks included.
pub(crate) fn natural_less(left: &[u8], right: &[u8]) -> bool {
    let left = natural_chunks(left);
    let right = natural_chunks(right);
    for index in 0..left.len() {
        let chunk = &left[index];
        let Some(other) = right.get(index) else {
            return false;
        };
        let chunk_number = std::str::from_utf8(chunk)
            .ok()
            .and_then(|chunk| chunk.parse::<i64>().ok());
        let other_number = std::str::from_utf8(other)
            .ok()
            .and_then(|other| other.parse::<i64>().ok());
        if let Some(chunk_number) = chunk_number {
            if let Some(other_number) = other_number {
                if chunk_number != other_number {
                    return chunk_number < other_number;
                }
            } else if chunk != other {
                return chunk < other;
            }
        } else if chunk != other {
            return chunk < other;
        }
        // The chunks match. `natsort` stops at whichever value runs out of
        // chunks first and calls that one the smaller.
        if index + 1 == left.len() {
            return true;
        }
        if index + 1 == right.len() {
            return false;
        }
    }
    false
}

/// Compares two label sets by the listed `label_names` in `order`.
///
/// This function returns the first non-equal label-value comparison, or
/// [`Ordering::Equal`] when every listed label is equal. A missing label
/// compares as the empty string.
///
/// Label values compare in NATURAL order, not byte order, because
/// `funcSortByLabel` compares them with `natsort`: `cpu="2"` therefore sorts
/// before `cpu="10"`. Two values that natural order calls neither equal nor
/// ascending compare as [`Ordering::Greater`], which is `funcSortByLabel`'s own
/// `return +1`.
pub(crate) fn compare_label_values(
    left: &Labels,
    right: &Labels,
    label_names: &[String],
    order: SortOrder,
) -> Ordering {
    for label_name in label_names {
        let left = left
            .get_value(label_name.as_str())
            .map_or(&[][..], crate::PromqlString::as_bytes);
        let right = right
            .get_value(label_name.as_str())
            .map_or(&[][..], crate::PromqlString::as_bytes);
        if left == right {
            continue;
        }
        let ordering = if natural_less(left, right) {
            Ordering::Less
        } else {
            Ordering::Greater
        };
        return match order {
            SortOrder::Ascending => ordering,
            SortOrder::Descending => ordering.reverse(),
        };
    }
    Ordering::Equal
}

pub(crate) fn apply_byte_label_join(
    samples: Vec<InstantSample>,
    dst: &PromqlString,
    separator: &PromqlString,
    source_names: &[PromqlString],
) -> Result<Vec<InstantSample>> {
    let source_names = source_names
        .iter()
        .map(|name| {
            name.utf8().filter(|name| !name.is_empty()).ok_or_else(|| {
                PromqlError::Exec("invalid source label name in label_join()".to_owned())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let dst = dst.utf8().filter(|name| !name.is_empty()).ok_or_else(|| {
        PromqlError::Exec("invalid destination label name in label_join()".to_owned())
    })?;
    Ok(apply_join_samples(
        samples,
        dst,
        separator.as_bytes(),
        &source_names,
    ))
}

pub(crate) fn apply_join_samples(
    samples: Vec<InstantSample>,
    dst_label: &str,
    separator: &[u8],
    source_names: &[&str],
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .map(|mut sample| {
            let mut value = Vec::new();
            for (index, name) in source_names.iter().enumerate() {
                if index > 0 {
                    value.extend_from_slice(separator);
                }
                if let Some(source) = sample.labels.get_value(name) {
                    value.extend_from_slice(source.as_bytes());
                }
            }
            sample.labels =
                set_label_value(&sample.labels, dst_label, crate::PromqlString::from(value));
            if dst_label == "__name__" {
                sample.drop_name = false;
            }
            sample
        })
        .collect()
}

pub(crate) fn apply_byte_label_replace(
    samples: Vec<InstantSample>,
    dst: &PromqlString,
    replacement: &PromqlString,
    src: &PromqlString,
    pattern: &PromqlString,
) -> Result<Vec<InstantSample>> {
    let pattern = pattern.utf8().ok_or_else(|| {
        PromqlError::Exec("invalid regular expression in label_replace()".to_owned())
    })?;
    // Go's regex decoder maps invalid bytes to RuneError, but capture indexes
    // still address the original bytes. Match its rune view, then map captures
    // back to their original byte ranges before expanding the replacement.
    let regex = Regex::new(&format!("^(?s:{pattern})$"))
        .map_err(|error| PromqlError::Exec(format!("invalid label_replace regex: {error}")))?;
    let dst = dst.utf8().filter(|name| !name.is_empty()).ok_or_else(|| {
        PromqlError::Exec("invalid destination label name in label_replace()".to_owned())
    })?;
    let capture_name = Regex::new(r"^[_\p{L}\p{Nd}]+").expect("static capture-name expression");
    Ok(samples
        .into_iter()
        .map(|mut sample| {
            let empty = PromqlString::default();
            let source = src
                .utf8()
                .and_then(|name| sample.labels.get_value(name))
                .unwrap_or(&empty);
            if let Some(captures) = regex.captures(source.as_str()) {
                let mut value = Vec::new();
                let replacement = replacement.as_bytes();
                let mut index = 0;
                while index < replacement.len() {
                    if replacement[index] != b'$' {
                        value.push(replacement[index]);
                        index += 1;
                        continue;
                    }
                    if replacement.get(index + 1) == Some(&b'$') {
                        value.push(b'$');
                        index += 2;
                        continue;
                    }
                    let wrapped = replacement.get(index + 1) == Some(&b'{');
                    let start = index + if wrapped { 2 } else { 1 };
                    let suffix = &replacement[start..];
                    let valid = match std::str::from_utf8(suffix) {
                        Ok(valid) => valid,
                        Err(error) => std::str::from_utf8(&suffix[..error.valid_up_to()])
                            .expect("valid UTF-8 prefix"),
                    };
                    let end = start + capture_name.find(valid).map_or(0, |name| name.end());
                    if start == end || (wrapped && replacement.get(end) != Some(&b'}')) {
                        value.push(b'$');
                        index += 1;
                        continue;
                    }
                    let name = std::str::from_utf8(&replacement[start..end])
                        .expect("validated capture name");
                    let capture = if name.bytes().all(|byte| byte.is_ascii_digit())
                        && (name == "0" || !name.starts_with('0'))
                    {
                        name.parse::<usize>()
                            .ok()
                            .and_then(|index| captures.get(index))
                    } else {
                        captures.name(name)
                    };
                    if let Some(capture) = capture {
                        value.extend_from_slice(
                            source
                                .bytes_for_json_range(capture.start(), capture.end())
                                .expect("regex captures end on decoded rune boundaries"),
                        );
                    }
                    index = end + usize::from(wrapped);
                }
                sample.labels =
                    set_label_value(&sample.labels, dst, crate::PromqlString::from(value));
                if dst == "__name__" {
                    sample.drop_name = false;
                }
            }
            sample
        })
        .collect())
}

#[cfg(test)]
/// Applies `label_replace(v, dst_label, replacement, src_label, regex)` to an
/// already-assembled instant vector.
///
/// `regex` is fully anchored as `^(?:<regex>)$`, as in Prometheus. For each
/// series whose `src_label` value matches `regex` in full, this function sets
/// the destination label to `replacement` with `$1` and `${name}` capture-group
/// expansion. A series that does not match passes through unchanged. This
/// function keeps `__name__` unless `dst_label == "__name__"`; these functions
/// never drop the metric name themselves. An empty expansion REMOVES
/// `dst_label`, because Prometheus writes the result through
/// `labels.Builder.Set`, which deletes a label rather than store it empty. The
/// removal then takes part in later collision checks exactly as Prometheus sees
/// it, which is what makes `label_replace(testmetric, "src", "", "", "")` a
/// duplicate-labelset failure.
///
/// # Errors
///
/// Returns [`PromqlError::Exec`] when `regex` is not a valid regular expression.
/// The error text matches the interpreter's error text.
pub fn apply_label_replace(
    samples: Vec<InstantSample>,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex: &str,
) -> Result<Vec<InstantSample>> {
    apply_byte_label_replace(
        samples,
        &dst_label.into(),
        &replacement.into(),
        &src_label.into(),
        &regex.into(),
    )
}
