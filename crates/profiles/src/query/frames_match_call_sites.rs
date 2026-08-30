pub(crate) fn frames_match_call_sites(
    frames: &[krabka_pprof::Frame],
    call_sites: &[String],
) -> bool {
    call_sites.iter().all(|site| {
        frames
            .iter()
            .any(|frame| frame.function == *site || frame.file == *site)
    })
}
