use super::*;

pub(crate) fn function_args<'a>(input: &'a str, name: &str) -> Result<Option<Vec<&'a str>>, ParseError> {
    let Some(rest) = input.strip_prefix(name) else {
        return Ok(None);
    };
    let rest = rest.trim_start();
    if !rest.starts_with('(') || !rest.ends_with(')') {
        return Ok(None);
    }
    let inner = &rest[1..rest.len() - 1];
    let mut starts = vec![0];
    let mut commas = Vec::new();
    scan_top_level(inner, |at| {
        if inner[at..].starts_with(',') {
            commas.push(at);
        }
    })?;
    starts.extend(commas.iter().map(|x| x + 1));
    let mut ends = commas;
    ends.push(inner.len());
    Ok(Some(
        starts
            .into_iter()
            .zip(ends)
            .map(|(a, b)| inner[a..b].trim())
            .collect(),
    ))
}
