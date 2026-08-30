use super::*;

pub type Labels = BTreeMap<String, String>;

#[must_use]
pub fn labels<const N: usize>(items: [(&str, &str); N]) -> Labels {
    items
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}
