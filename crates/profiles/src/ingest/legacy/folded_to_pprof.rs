use super::*;

pub(crate) fn folded_to_pprof(
    name: &str,
    sample_unit: &str,
    body: &str,
) -> Result<PprofProfile, ProfilesError> {
    let mut stacks = BTreeMap::<Vec<(String, i32)>, i64>::new();
    for (line_no, raw_line) in body.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (stack, value) = line.rsplit_once(char::is_whitespace).ok_or_else(|| {
            ProfilesError::Decode(format!("folded line {} missing value", line_no + 1))
        })?;
        let value = value.parse::<i64>().map_err(|err| {
            ProfilesError::Decode(format!(
                "folded line {} has invalid value: {err}",
                line_no + 1
            ))
        })?;
        let frames = stack
            .split(';')
            .filter(|frame| !frame.is_empty())
            .map(|frame| (frame.to_string(), 0))
            .collect::<Vec<_>>();
        if frames.is_empty() {
            return Err(ProfilesError::Decode(format!(
                "folded line {} has empty stack",
                line_no + 1
            )));
        }
        *stacks.entry(frames).or_default() += value;
    }

    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "folded profile has no samples".to_string(),
        ));
    }
    Ok(stacks_to_pprof(name, "samples", sample_unit, stacks))
}
