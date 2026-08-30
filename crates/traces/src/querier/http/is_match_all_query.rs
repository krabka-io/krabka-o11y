
pub(crate) fn is_match_all_query(query: &str) -> bool {
    query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .eq("{}".chars())
}
