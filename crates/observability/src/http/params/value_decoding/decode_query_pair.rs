use super::{HttpQueryError, decode_form_component};

/// One `key=value` pair of a URL query string or form body, percent-decoded.
pub(crate) struct DecodedQueryPair {
    pub(crate) key: String,
    pub(crate) value: String,
}

/// Decodes one `&`-separated query pair; a pair without `=` has an empty value.
pub(crate) fn decode_query_pair(pair: &str) -> Result<DecodedQueryPair, HttpQueryError> {
    let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
    Ok(DecodedQueryPair {
        key: decode_form_component(key)?,
        value: decode_form_component(value)?,
    })
}
