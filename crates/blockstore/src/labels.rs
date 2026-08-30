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
        for (_name, left, right) in [
            (
                "embedded equals sign",
                Labels::from_pairs([("a", "b=c")]),
                Labels::from_pairs([("a=b", "c")]),
            ),
            (
                "embedded newline",
                Labels::from_pairs([("x", "y"), ("z", "")]),
                Labels::from_pairs([("x", "y\nz=")]),
            ),
        ] {
            assert2::assert!(left.fingerprint() != right.fingerprint());
        }
    }

    #[test]
    fn get_and_iter_round_trip() {
        let mut l = Labels::new();
        assert2::assert!(&l == &Labels::new());
        l.insert("app", "api");
        assert2::assert!(&l == &Labels::from_pairs([("app", "api")]));
        assert2::assert!(l.get("app") == Some("api"));
        assert2::assert!(l.get("missing") == None);
        l.insert("env", "prod");
        let pairs = l
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        assert2::assert!(&l == &Labels::from_pairs([("app", "api"), ("env", "prod")]));
        assert2::assert!(pairs == vec![("app", "api"), ("env", "prod")]);
    }

    #[test]
    fn from_iterator_preserves_pairs() {
        let labels = vec![
            ("app".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]
        .into_iter()
        .collect::<Labels>();

        let pairs = labels
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        assert2::assert!(pairs == vec![("app", "api"), ("env", "prod")]);
    }
}

// === split-modules: generated submodules ===
mod labels;
mod series_fingerprint;

pub use labels::Labels;
pub use series_fingerprint::SeriesFingerprint;
