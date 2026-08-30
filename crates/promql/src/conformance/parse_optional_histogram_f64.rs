use super::{BTreeMap, Line, Result, parse_float};

pub(crate) fn parse_optional_histogram_f64(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<f64>> {
    fields
        .get(name)
        .map(|value| parse_float(value, line))
        .transpose()
}
