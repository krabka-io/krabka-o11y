use super::ProfilesError;

pub(crate) fn split_app_labels(s: &str) -> Result<(String, Vec<(String, String)>), ProfilesError> {
    let Some(open) = s.find('{') else {
        return Ok((s.to_string(), Vec::new()));
    };
    let name = s[..open].to_string();
    let inner = s[open + 1..]
        .strip_suffix('}')
        .ok_or_else(|| ProfilesError::Invalid("unterminated label set".to_string()))?;
    let mut labels = Vec::new();
    for pair in inner.split(',').filter(|part| !part.is_empty()) {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| ProfilesError::Invalid("bad label pair".to_string()))?;
        labels.push((
            key.trim().to_string(),
            value.trim().trim_matches('"').to_string(),
        ));
    }
    Ok((name, labels))
}
