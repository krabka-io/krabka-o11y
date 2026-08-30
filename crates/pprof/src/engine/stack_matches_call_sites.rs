use super::Frame;

pub(crate) fn stack_matches_call_sites(frames: &[Frame], call_sites: &[String]) -> bool {
    call_sites.iter().all(|site| {
        frames
            .iter()
            .any(|frame| frame.function == *site || frame.file == *site)
    })
}
