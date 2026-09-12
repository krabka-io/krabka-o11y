use super::{
    DEFAULT_SPY_NAME, IngestFormat, IngestQuery, ProfilesError, parse_unix_time_ms,
    split_app_labels, urldecode,
};

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
    let mut spy_name = DEFAULT_SPY_NAME.to_string();
    let mut jfr_event = "wall".to_string();

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
            "spyName" if !value.is_empty() => {
                spy_name = value;
            }
            "event" if !value.is_empty() => jfr_event = value,
            _ => {}
        }
    }

    if name.is_empty() {
        return Err(ProfilesError::Invalid("missing ?name".to_string()));
    }

    let profile_type_suffix = name
        .rsplit_once('.')
        .filter(|(_, suffix)| {
            matches!(
                *suffix,
                "alloc_objects" | "alloc_space" | "inuse_objects" | "inuse_space"
            )
        })
        .map(|(application, suffix)| (application.to_string(), suffix.to_string()));
    if let Some((application, _)) = &profile_type_suffix {
        name.clone_from(application);
    }

    Ok(IngestQuery {
        name,
        profile_type_suffix: profile_type_suffix.map(|(_, suffix)| suffix),
        labels,
        format,
        sample_rate,
        units,
        from_ms,
        until_ms,
        spy_name,
        jfr_event,
    })
}
