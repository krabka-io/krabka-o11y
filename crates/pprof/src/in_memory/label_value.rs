use super::SampleRow;

pub(crate) fn label_value<'a>(row: &'a SampleRow, name: &str) -> Option<&'a str> {
    row.labels
        .iter()
        .find(|label| label.name == name)
        .map(|label| label.value.as_str())
}
