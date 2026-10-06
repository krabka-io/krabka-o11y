pub(crate) fn frames_match_call_sites(
    frames: &[krabka_pprof::Frame],
    call_sites: &[String],
) -> bool {
    krabka_pprof::stack_matches_call_sites(frames, call_sites)
}
