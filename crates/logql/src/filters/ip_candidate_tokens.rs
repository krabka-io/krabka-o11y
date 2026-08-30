
pub(crate) fn ip_candidate_tokens(line: &str) -> impl Iterator<Item = &str> {
    line.split(|ch: char| !(ch.is_ascii_hexdigit() || ch == '.' || ch == ':'))
        .filter(|candidate| !candidate.is_empty())
}
