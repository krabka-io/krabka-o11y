use super::{BTreeMap, PprofProfile, ProfilesError, stacks_to_pprof};

pub(crate) fn speedscope_to_pprof(
    name: &str,
    default_unit: &str,
    body: &[u8],
) -> Result<PprofProfile, ProfilesError> {
    let json: serde_json::Value = serde_json::from_slice(body)
        .map_err(|err| ProfilesError::Decode(format!("speedscope profile is not JSON: {err}")))?;
    let frames = json
        .pointer("/shared/frames")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| ProfilesError::Decode("speedscope shared.frames missing".to_string()))?;
    let frame_names = frames
        .iter()
        .enumerate()
        .map(|(idx, frame)| {
            frame
                .get("name")
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    ProfilesError::Decode(format!("speedscope shared.frames[{idx}].name missing"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let profiles = json
        .get("profiles")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| ProfilesError::Decode("speedscope profiles missing".to_string()))?;
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    let mut sample_unit = default_unit.to_string();

    for (profile_idx, profile) in profiles.iter().enumerate() {
        let profile_type = profile
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if profile_type != "sampled" {
            continue;
        }
        if let Some(unit) = profile
            .get("unit")
            .and_then(serde_json::Value::as_str)
            .filter(|unit| !unit.is_empty())
        {
            sample_unit = unit.to_string();
        }
        let samples = profile
            .get("samples")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                ProfilesError::Decode(format!(
                    "speedscope profiles[{profile_idx}].samples missing"
                ))
            })?;
        let weights = profile
            .get("weights")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        for (sample_idx, sample) in samples.iter().enumerate() {
            let stack = sample.as_array().ok_or_else(|| {
                ProfilesError::Decode(format!(
                    "speedscope profiles[{profile_idx}].samples[{sample_idx}] must be an array"
                ))
            })?;
            let mut frames = Vec::new();
            for frame in stack {
                let frame_idx = frame.as_u64().ok_or_else(|| {
                    ProfilesError::Decode(format!(
                        "speedscope profiles[{profile_idx}].samples[{sample_idx}] frame index must be unsigned"
                    ))
                })?;
                let name = frame_names
                    .get(usize::try_from(frame_idx).map_err(|err| {
                        ProfilesError::Decode(format!(
                            "speedscope frame index does not fit usize: {err}"
                        ))
                    })?)
                    .ok_or_else(|| {
                        ProfilesError::Decode(format!(
                            "speedscope frame index {frame_idx} is out of bounds"
                        ))
                    })?;
                frames.push((name.clone(), 0));
            }
            if frames.is_empty() {
                return Err(ProfilesError::Decode(format!(
                    "speedscope profiles[{profile_idx}].samples[{sample_idx}] has empty stack"
                )));
            }
            let value = weights
                .get(sample_idx)
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(1);
            *stacks.entry(frames).or_default() += value;
        }
    }

    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "speedscope profile has no sampled stacks".to_string(),
        ));
    }
    Ok(stacks_to_pprof(name, "samples", &sample_unit, stacks))
}
