use super::{HeaderMap, LOKI_RESPONSE_ENCODING_FLAGS_HEADER};

/// The encoding flags a request asked for, in the order it named them.
///
/// Loki splits the header on commas and neither trims nor lowercases the
/// pieces, and it echoes them back exactly as given -- including a flag it does
/// not know. Only the first header line counts when a request repeats the
/// header. An absent or empty header asks for nothing at all.
pub(crate) fn loki_encoding_flags(headers: &HeaderMap) -> Vec<String> {
    let Some(header) = headers
        .get(LOKI_RESPONSE_ENCODING_FLAGS_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|header| !header.is_empty())
    else {
        return Vec::new();
    };
    header.split(',').map(str::to_string).collect()
}
