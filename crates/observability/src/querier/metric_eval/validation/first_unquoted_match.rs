/// The first `Some` that `matcher` returns for a character of `query` outside
/// every double-quoted string, given the character's byte index.
pub(crate) fn first_unquoted_match<T>(
    query: &str,
    mut matcher: impl FnMut(usize, char) -> Option<T>,
) -> Option<T> {
    let mut in_string = false;
    let mut escaped = false;
    for (index, ch) in query.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        if ch == '"' {
            in_string = true;
            continue;
        }
        if let Some(found) = matcher(index, ch) {
            return Some(found);
        }
    }
    None
}
