/// Convert a `usize` command-line limit into the `u64` the shared limits hold.
///
/// `usize::MAX` is the command line's spelling of "no limit", and zero is what
/// [`krabka_traces::Limits`] reads as unlimited, so the sentinel maps to zero.
/// Every other value maps to itself.
pub(crate) fn u64_limit_from_usize(value: usize) -> u64 {
    if value == usize::MAX {
        0
    } else {
        u64::try_from(value).unwrap_or(u64::MAX)
    }
}
