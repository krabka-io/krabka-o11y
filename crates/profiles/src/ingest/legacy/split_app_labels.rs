use super::ProfilesError;

pub(crate) struct SplitAppLabels {
    pub(crate) name: String,
    pub(crate) labels: Vec<krabka_blockstore::LabelPair>,
}

pub(crate) fn split_app_labels(s: &str) -> Result<SplitAppLabels, ProfilesError> {
    let Some(open) = s.find('{') else {
        return Ok(SplitAppLabels {
            name: s.to_string(),
            labels: Vec::new(),
        });
    };
    let name = s[..open].to_string();
    let inner = s[open + 1..]
        .strip_suffix('}')
        .ok_or_else(|| ProfilesError::Invalid("unterminated label set".to_string()))?;
    let mut labels = Vec::new();
    for pair in inner.split(',').filter(|part| !part.is_empty()) {
        let parts = pair
            .split_once('=')
            .ok_or_else(|| ProfilesError::Invalid("bad label pair".to_string()))?;
        labels.push(krabka_blockstore::LabelPair::new(
            parts.0.trim(),
            parts.1.trim().trim_matches('"'),
        ));
    }
    Ok(SplitAppLabels { name, labels })
}
