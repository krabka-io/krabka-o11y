use super::{ParseError, scan_top_level};

pub(crate) fn function_args<'a>(
    input: &'a str,
    name: &str,
) -> Result<Option<Vec<&'a str>>, ParseError> {
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
    let mut args = Vec::with_capacity(starts.len());
    for index in 0..starts.len() {
        let start = starts[index];
        let end = ends[index];
        args.push(inner[start..end].trim());
    }
    Ok(Some(args))
}
