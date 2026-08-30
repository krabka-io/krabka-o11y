use super::{HeaderMap, HeaderValue};

pub(crate) fn insert_written_header(headers: &mut HeaderMap, name: &'static str, value: u64) {
    headers.insert(
        name,
        HeaderValue::from_str(&value.to_string()).expect("u64 header value"),
    );
}
