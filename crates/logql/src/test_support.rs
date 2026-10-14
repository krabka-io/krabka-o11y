use crate::Labels;

#[derive(Debug)]
pub(crate) struct LabelPair<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

impl<'a> LabelPair<'a> {
    pub(crate) const fn new(name: &'a str, value: &'a str) -> Self {
        Self { name, value }
    }
}

pub(crate) fn labels(pairs: &[LabelPair<'_>]) -> Labels {
    let mut labels = Labels::new();
    for pair in pairs {
        labels.insert(pair.name.to_string(), pair.value.to_string());
    }
    labels
}
