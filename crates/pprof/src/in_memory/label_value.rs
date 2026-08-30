use super::SampleRow;

pub(crate) fn label_value<'a>(row: &'a SampleRow, name: &str) -> Option<&'a str> {
    row.labels
        .iter()
        .find(|(label_name, _)| label_name == name)
        .map(|(_, value)| value.as_str())
}
