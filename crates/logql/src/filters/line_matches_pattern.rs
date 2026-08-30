
pub(crate) fn line_matches_pattern(line: &str, pattern: &str) -> bool {
    let mut remaining = line;
    let mut matched_any_literal = false;
    for literal in pattern.split("<_>").filter(|literal| !literal.is_empty()) {
        let Some(offset) = remaining.find(literal) else {
            return false;
        };
        matched_any_literal = true;
        remaining = &remaining[offset + literal.len()..];
    }
    matched_any_literal || pattern.contains("<_>")
}
