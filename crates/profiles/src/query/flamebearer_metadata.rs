use super::{ProfileType, json};

pub(crate) fn flamebearer_metadata(format: &str, profile_type: &str) -> serde_json::Value {
    match ProfileType::parse(profile_type) {
        Ok(parsed) => json!({
            "format": format,
            "spyName": parsed.name,
            "sampleRate": 100,
            "units": parsed.sample_unit,
            "name": profile_type,
        }),
        Err(_) => json!({
            "format": format,
            "spyName": "",
            "sampleRate": 100,
            "units": "",
            "name": profile_type,
        }),
    }
}
