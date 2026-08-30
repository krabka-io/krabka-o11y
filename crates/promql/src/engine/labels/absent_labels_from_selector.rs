use super::{Labels, VectorSelector, absent_labels_from_matchers, label_matcher_sets};

pub(crate) fn absent_labels_from_selector(selector: &VectorSelector) -> Labels {
    let matcher_sets = label_matcher_sets(selector);
    if matcher_sets.len() == 1 {
        return absent_labels_from_matchers(&matcher_sets[0]);
    }
    Labels::new()
}
