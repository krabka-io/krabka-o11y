use super::{PprofProfile, ProfilesError, TextStackFormat, parse_text_stacks, stacks_to_pprof};

pub(crate) fn lines_to_pprof(
    name: &str,
    sample_unit: &str,
    body: &str,
) -> Result<PprofProfile, ProfilesError> {
    let stacks = parse_text_stacks(body, TextStackFormat::Lines)?;
    Ok(stacks_to_pprof(name, "samples", sample_unit, stacks))
}
