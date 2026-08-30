use super::*;

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn parse_ingest_query(query: &str) -> Result<IngestQuery, ProfilesError> {
    let mut name = String::new();
    let mut labels = Vec::new();
    let mut format = IngestFormat::Groups;
    let mut sample_rate = 100;
    let mut units = "count".to_string();
    let mut from_ms = None;
    let mut until_ms = None;

    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let value = urldecode(value);
        match key {
            "name" => {
                (name, labels) = split_app_labels(&value)?;
            }
            "format" => {
                format = match value.as_str() {
                    "pprof" => IngestFormat::Pprof,
                    "jfr" => IngestFormat::Jfr,
                    "trie" => IngestFormat::Trie,
                    "tree" => IngestFormat::Tree,
                    "lines" => IngestFormat::Lines,
                    "speedscope" => IngestFormat::Speedscope,
                    _ => IngestFormat::Groups,
                };
            }
            "sampleRate" => {
                sample_rate = value.parse().map_err(|error| {
                    ProfilesError::Invalid(format!("invalid sampleRate `{value}`: {error}"))
                })?;
                if sample_rate == 0 {
                    return Err(ProfilesError::Invalid(
                        "sampleRate must be positive".to_string(),
                    ));
                }
            }
            "units" => {
                if !value.is_empty() {
                    units = value;
                }
            }
            "from" => {
                from_ms = Some(parse_unix_time_ms(&value)?);
            }
            "until" => {
                until_ms = Some(parse_unix_time_ms(&value)?);
            }
            _ => {}
        }
    }

    if name.is_empty() {
        return Err(ProfilesError::Invalid("missing ?name".to_string()));
    }

    Ok(IngestQuery {
        name,
        labels,
        format,
        sample_rate,
        units,
        from_ms,
        until_ms,
    })
}
