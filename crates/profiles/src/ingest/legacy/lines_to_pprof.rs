use super::{BTreeMap, PprofProfile, ProfilesError, stacks_to_pprof};

pub(crate) fn lines_to_pprof(
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
        let frames = line
            .split(';')
            .filter(|frame| !frame.is_empty())
            .map(|frame| (frame.to_string(), 0))
            .collect::<Vec<_>>();
        if frames.is_empty() {
            return Err(ProfilesError::Decode(format!(
                "lines profile line {} has empty stack",
                line_no + 1
            )));
        }
        *stacks.entry(frames).or_default() += 1;
    }

    if stacks.is_empty() {
        return Err(ProfilesError::Decode(
            "lines profile has no samples".to_string(),
        ));
    }
    Ok(stacks_to_pprof(name, "samples", sample_unit, stacks))
}
