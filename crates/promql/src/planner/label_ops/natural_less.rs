use super::natural_chunks;

/// Whether `left` precedes `right` in natural order.
///
/// `sort_by_label` orders label values with `facette/natsort`, so `cpu="2"`
/// sorts before `cpu="10"`: each value splits into runs of digits and runs of
/// non-digits, and a pair of digit runs compares as integers. A digit run too
/// long for an `i64` falls back to a byte comparison, matching `strconv.Atoi`'s
/// overflow error.
///
/// This answers only "does `left` precede `right`", the one question
/// `natsort.Compare` answers, so that the caller can reproduce
/// `funcSortByLabel`'s ordering exactly, quirks included.
pub(crate) fn natural_less(left: &[u8], right: &[u8]) -> bool {
    let left = natural_chunks(left);
    let right = natural_chunks(right);
    for (index, chunk) in left.iter().enumerate() {
        let Some(other) = right.get(index) else {
            return false;
        };
        match (
            std::str::from_utf8(chunk)
                .ok()
                .and_then(|chunk| chunk.parse::<i64>().ok()),
            std::str::from_utf8(other)
                .ok()
                .and_then(|other| other.parse::<i64>().ok()),
        ) {
            (Some(chunk), Some(other)) if chunk != other => return chunk < other,
            (Some(_), Some(_)) => {}
            _ if chunk != other => return chunk < other,
            _ => {}
        }
        // The chunks match. `natsort` stops at whichever value runs out of
        // chunks first and calls that one the smaller.
        if index + 1 == left.len() {
            return true;
        }
        if index + 1 == right.len() {
            return false;
        }
    }
    false
}
