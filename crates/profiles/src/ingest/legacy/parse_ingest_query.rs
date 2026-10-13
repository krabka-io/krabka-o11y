use super::{
    DEFAULT_SPY_NAME, IngestFormat, IngestQuery, ProfilesError, parse_unix_time_ms,
    split_app_labels, urldecode,
};

struct QueryParameter<'a> {
    key: &'a str,
    value: &'a str,
}

fn query_parameter(pair: &str) -> QueryParameter<'_> {
    match pair.find('=') {
        Some(separator) => QueryParameter {
            key: &pair[..separator],
            value: &pair[separator + 1..],
        },
        None => QueryParameter {
            key: pair,
            value: "",
        },
    }
}

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
        let parameter = query_parameter(pair);
        let key = parameter.key;
        let value = urldecode(parameter.value);
        match key {
            "name" => {
                let parsed = split_app_labels(&value)?;
                name = parsed.name;
                labels = parsed.labels;
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

    let profile_type_suffix = name.rfind('.').and_then(|separator| {
        let suffix = &name[separator + 1..];
        matches!(
            suffix,
            "alloc_objects" | "alloc_space" | "inuse_objects" | "inuse_space"
        )
        .then(|| suffix.to_string())
    });
    if profile_type_suffix.is_some() {
        name.truncate(name.rfind('.').expect("validated profile suffix"));
    }

    Ok(IngestQuery {
        name,
        profile_type_suffix,
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
