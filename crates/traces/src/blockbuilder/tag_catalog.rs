use super::{BTreeMap, BTreeSet};

/// The tag names and per-tag values a trace block's index entry carries,
/// borrowed while a block builder or the compactor collects them.
pub(crate) struct TagCatalog<'a> {
    pub(crate) names: &'a mut BTreeSet<String>,
    pub(crate) values: &'a mut BTreeMap<String, BTreeSet<String>>,
}

impl TagCatalog<'_> {
    /// Records `tag` as a name and `value` as one of its values.
    pub(crate) fn insert(&mut self, tag: &str, value: String) {
        self.names.insert(tag.to_string());
        self.values
            .entry(tag.to_string())
            .or_default()
            .insert(value);
    }
}
