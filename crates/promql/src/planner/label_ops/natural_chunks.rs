/// Splits a label value into the runs of digits and runs of non-digits that
/// natural order compares.
///
/// This is `natsort`'s `chunkify`, whose `(\d+|\D+)` is ASCII-only in RE2, so a
/// non-ASCII digit stays in a non-digit run.
pub(crate) fn natural_chunks(value: &str) -> Vec<&str> {
    let mut chunks = Vec::new();
    let mut rest = value;
    while !rest.is_empty() {
        let digits = rest.starts_with(|ch: char| ch.is_ascii_digit());
        let end = rest
            .find(|ch: char| ch.is_ascii_digit() != digits)
            .unwrap_or(rest.len());
        let (chunk, tail) = rest.split_at(end);
        chunks.push(chunk);
        rest = tail;
    }
    chunks
}
