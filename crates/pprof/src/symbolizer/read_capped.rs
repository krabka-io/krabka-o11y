use super::read_capped_reader;

/// Read an HTTP body into memory.
///
/// This function stops and returns `None` as soon as the accumulated size is
/// more than `cap` bytes. It avoids the unbounded `response.bytes()`
/// allocation.
pub(crate) fn read_capped(mut response: reqwest::blocking::Response, cap: u64) -> Option<Vec<u8>> {
    read_capped_reader(&mut response, cap)
}
