use super::{BTreeMap, Line, Result, parse_error, parse_float};

pub(crate) fn parse_optional_histogram_buckets(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<Vec<f64>>> {
    let Some(value) = fields.get(name) else {
        return Ok(None);
    };
    let bucket_values = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| {
            parse_error(
                line,
                format!("histogram {name} must be enclosed in `[` and `]`"),
            )
        })?;
    if bucket_values.trim().is_empty() {
        return Ok(Some(Vec::new()));
    }
    let values = bucket_values
        .split_whitespace()
        .map(|bucket| parse_float(bucket, line))
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(values))
}
