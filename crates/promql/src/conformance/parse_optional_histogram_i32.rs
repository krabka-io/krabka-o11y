use super::{BTreeMap, Line, Result, parse_error};

pub(crate) fn parse_optional_histogram_i32(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<i32>> {
    fields
        .get(name)
        .map(|value| {
            value.parse::<i32>().map_err(|error| {
                parse_error(line, format!("invalid histogram {name} `{value}`: {error}"))
            })
        })
        .transpose()
}
