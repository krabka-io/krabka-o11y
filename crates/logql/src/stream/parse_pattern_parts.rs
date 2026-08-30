use super::{PatternPart, ParseError, pattern_parse_error};

pub(crate) fn parse_pattern_parts(pattern: &str) -> Result<Vec<PatternPart>, ParseError> {
    let mut pos = 0;
    let mut parts = Vec::new();
    let mut has_named_capture = false;
    let mut previous_capture = false;
    let mut separator_since_capture = String::new();

    while let Some(open_offset) = pattern[pos..].find('<') {
        let literal_start = pos;
        let open = pos.saturating_add(open_offset);
        let literal = &pattern[literal_start..open];
        if !literal.is_empty() {
            separator_since_capture.push_str(literal);
            parts.push(PatternPart::Literal(literal.to_string()));
        }

        let capture_start = open.saturating_add(1);
        let close_offset = pattern[capture_start..]
            .find('>')
            .ok_or_else(|| pattern_parse_error("expected closing pattern capture"))?;
        let close = capture_start.saturating_add(close_offset);
        let name = &pattern[capture_start..close];
        if name.is_empty() {
            return Err(pattern_parse_error("expected pattern capture name"));
        }
        if previous_capture && !separator_since_capture.chars().any(char::is_whitespace) {
            return Err(pattern_parse_error(
                "expected whitespace between pattern captures",
            ));
        }
        if name != "_" {
            has_named_capture = true;
        }
        parts.push(PatternPart::Capture(name.to_string()));
        previous_capture = true;
        separator_since_capture.clear();
        pos = close.saturating_add(1);
    }

    let literal = &pattern[pos..];
    if !literal.is_empty() {
        separator_since_capture.push_str(literal);
        parts.push(PatternPart::Literal(literal.to_string()));
    }

    if !has_named_capture {
        return Err(pattern_parse_error(
            "pattern parser requires at least one named capture",
        ));
    }
    Ok(parts)
}
