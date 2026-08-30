use super::{ProfilesError, SampleTypeConfig};

pub(crate) fn parse_sample_type_config(raw: &[u8]) -> Result<SampleTypeConfig, ProfilesError> {
    let config: SampleTypeConfig = serde_json::from_slice(raw)
        .map_err(|err| ProfilesError::Decode(format!("sample_type_config is not JSON: {err}")))?;
    if config
        .aggregation
        .as_deref()
        .is_some_and(|aggregation| !aggregation.eq_ignore_ascii_case("sum"))
    {
        return Err(ProfilesError::Invalid(
            "sample_type_config aggregation must be `sum`".to_string(),
        ));
    }
    if config.sampled == Some(false) {
        return Err(ProfilesError::Invalid(
            "sample_type_config sampled=false is not supported".to_string(),
        ));
    }
    Ok(config)
}
