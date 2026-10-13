use std::collections::HashMap;

use prost::Message as _;

use super::{JfrLabels, LabelsSnapshot, ProfilesError};

struct ContextLabels {
    context_id: i64,
    labels: Vec<krabka_blockstore::LabelPair>,
}

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
            .map(|entry| {
                let key = entry.0;
                let value = entry.1;
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
                Ok(krabka_blockstore::LabelPair::new(key, value))
            })
            .collect::<Result<_, _>>()?;
        return Ok(JfrLabels {
            global,
            contexts: HashMap::default(),
        });
    }

    let snapshot = LabelsSnapshot::decode(raw)
        .map_err(|err| ProfilesError::Decode(format!("jfr labels protobuf is invalid: {err}")))?;
    let contexts = snapshot
        .contexts
        .into_iter()
        .map(|context_entry| {
            let context_id = context_entry.0;
            let context = context_entry.1;
            let mut labels = Vec::new();
            for label_entry in context.labels {
                let key = label_entry.0;
                let value = label_entry.1;
                let Some(name) = snapshot.strings.get(&key) else {
                    continue;
                };
                let Some(value) = snapshot.strings.get(&value) else {
                    continue;
                };
                labels.push(krabka_blockstore::LabelPair::new(
                    name.clone(),
                    value.clone(),
                ));
            }
            ContextLabels { context_id, labels }
        })
        .fold(HashMap::default(), |mut contexts, entry| {
            contexts.insert(entry.context_id, entry.labels);
            contexts
        });
    Ok(JfrLabels {
        global: Vec::new(),
        contexts,
    })
}
