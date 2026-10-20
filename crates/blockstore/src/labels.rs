use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests {

    use super::*;

    /// `len` and `is_empty` are read all over the query paths to decide whether
    /// a series carries any labels at all, and nothing asserted either. A `len`
    /// answering a constant, or an `is_empty` stuck at one answer, describes
    /// every series as the same shape.
    #[test]
    fn len_and_is_empty_track_the_entries() {
        let mut labels = Labels::new();
        assert2::check!((labels.len(), labels.is_empty()) == (0, true));

        labels.insert("app", "api");
        assert2::check!((labels.len(), labels.is_empty()) == (1, false));

        labels.insert("region", "us-east");
        assert2::check!((labels.len(), labels.is_empty()) == (2, false));

        // Re-inserting a name replaces it rather than growing the set.
        labels.insert("app", "web");
        assert2::check!((labels.len(), labels.is_empty()) == (2, false));
    }

    #[test]
    fn fingerprint_is_order_independent() {
        let mut a = Labels::new();
        a.insert("app", "api");
        a.insert("env", "prod");
        let mut b = Labels::new();
        b.insert("env", "prod");
        b.insert("app", "api");
        assert2::assert!(a.fingerprint() == b.fingerprint());
    }

    #[test]
    fn fingerprint_distinguishes_values() {
        let mut a = Labels::new();
        a.insert("app", "api");
        let mut b = Labels::new();
        b.insert("app", "web");
        assert2::assert!(a.fingerprint() != b.fingerprint());
    }

    #[test]
    fn fingerprint_is_injective_across_delimiter_ambiguity() {
        let embedded_equals_left = Labels::from_pairs([LabelPair::new("a", "b=c")]);
        let embedded_equals_right = Labels::from_pairs([LabelPair::new("a=b", "c")]);
        assert2::assert!(embedded_equals_left.fingerprint() != embedded_equals_right.fingerprint());
        let embedded_newline_left =
            Labels::from_pairs([LabelPair::new("x", "y"), LabelPair::new("z", "")]);
        let embedded_newline_right = Labels::from_pairs([LabelPair::new("x", "y\nz=")]);
        assert2::assert!(
            embedded_newline_left.fingerprint() != embedded_newline_right.fingerprint()
        );
    }

    #[test]
    fn get_and_iter_round_trip() {
        let mut l = Labels::new();
        assert2::assert!(&l == &Labels::new());
        l.insert("app", "api");
        assert2::assert!(&l == &Labels::from_pairs([LabelPair::new("app", "api")]));
        assert2::assert!(l.get("app") == Some("api"));
        assert2::assert!(l.get("missing") == None);
        l.insert("env", "prod");
        let pairs = l.iter().collect::<Vec<_>>();
        assert2::assert!(
            &l == &Labels::from_pairs([
                LabelPair::new("app", "api"),
                LabelPair::new("env", "prod"),
            ])
        );
        assert2::assert!(
            pairs
                == vec![
                    LabelPairRef {
                        name: "app",
                        value: "api"
                    },
                    LabelPairRef {
                        name: "env",
                        value: "prod"
                    },
                ]
        );
    }

    #[test]
    fn owned_iteration_keeps_sorted_pairs_and_duplicate_replacement() {
        let empty = Labels::from_pairs(Vec::new());
        assert2::assert!(empty.into_iter().collect::<Vec<_>>().is_empty());
        let labels = Labels::from_pairs([
            LabelPair::new("z", "discard"),
            LabelPair::new("a", "first"),
            LabelPair::new("z", "last"),
        ]);
        assert2::assert!(
            labels.into_iter().collect::<Vec<_>>()
                == vec![LabelPair::new("a", "first"), LabelPair::new("z", "last"),]
        );
        let labels = Labels::from_pairs([
            LabelPair::new("α", "v\0x"),
            LabelPair::new("", ""),
            LabelPair::new("a=b", "c\nd"),
        ]);
        assert2::assert!(
            labels.into_iter().collect::<Vec<_>>()
                == vec![
                    LabelPair::new("", ""),
                    LabelPair::new("a=b", "c\nd"),
                    LabelPair::new("α", "v\0x"),
                ]
        );
    }

    #[test]
    fn from_iterator_preserves_pairs() {
        let labels = vec![LabelPair::new("app", "api"), LabelPair::new("env", "prod")]
            .into_iter()
            .collect::<Labels>();

        let pairs = labels.iter().collect::<Vec<_>>();
        assert2::assert!(
            pairs
                == vec![
                    LabelPairRef {
                        name: "app",
                        value: "api"
                    },
                    LabelPairRef {
                        name: "env",
                        value: "prod"
                    },
                ]
        );
    }
}

mod labels_type;

pub use labels_type::{LabelPair, LabelPairRef, Labels, LabelsIntoIter, LabelsIter, SeriesLabels};

/// 64-bit fingerprint of a label set. Stable across process runs.
pub type SeriesFingerprint = u64;
