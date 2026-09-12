use prost::Message as _;

use super::{JfrLabels, LabelsSnapshot, ProfilesError};

pub(crate) fn parse_labels_part(raw: &[u8]) -> Result<JfrLabels, ProfilesError> {
    if raw.is_empty() {
        return Ok(JfrLabels::default());
    }
    if !matches!(raw.first(), Some(0x0a | 0x12)) {
        let raw = raw.trim_ascii_start();
        let json: serde_json::Value = serde_json::from_slice(raw)
            .map_err(|err| ProfilesError::Decode(format!("jfr labels part is not JSON: {err}")))?;
        let object = json.as_object().ok_or_else(|| {
            ProfilesError::Decode("jfr labels part must be a JSON object".to_string())
        })?;
        let global = object
            .iter()
            .map(|(key, value)| {
                let value = match value {
                    serde_json::Value::String(value) => value.clone(),
                    serde_json::Value::Number(value) => value.to_string(),
                    serde_json::Value::Bool(value) => value.to_string(),
                    serde_json::Value::Null => String::new(),
                    _ => {
                        return Err(ProfilesError::Decode(format!(
                            "jfr label `{key}` must be a scalar"
                        )));
                    }
                };
                Ok((key.clone(), value))
            })
            .collect::<Result<_, _>>()?;
        return Ok(JfrLabels {
            global,
            contexts: Default::default(),
        });
    }

    let snapshot = LabelsSnapshot::decode(raw)
        .map_err(|err| ProfilesError::Decode(format!("jfr labels protobuf is invalid: {err}")))?;
    let contexts = snapshot
        .contexts
        .into_iter()
        .map(|(context_id, context)| {
            let labels = context
                .labels
                .into_iter()
                .filter_map(|(key, value)| {
                    Some((
                        snapshot.strings.get(&key)?.clone(),
                        snapshot.strings.get(&value)?.clone(),
                    ))
                })
                .collect();
            (context_id, labels)
        })
        .collect();
    Ok(JfrLabels {
        global: Vec::new(),
        contexts,
    })
}
