pub(crate) fn content_length_within_cap(content_length: Option<u64>, cap: u64) -> bool {
    content_length.is_none_or(|len| len <= cap)
}
