use super::ProfileError;

pub(crate) fn unescape_quoted(input: &str) -> Result<String, ProfileError> {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(next) = chars.next() else {
            return Err(ProfileError::Plan(
                "trailing escape in matcher value".to_string(),
            ));
        };
        match next {
            '"' | '\\' => out.push(next),
            'n' => out.push('\n'),
            't' => out.push('\t'),
            other => {
                out.push('\\');
                out.push(other);
            }
        }
    }
    Ok(out)
}
