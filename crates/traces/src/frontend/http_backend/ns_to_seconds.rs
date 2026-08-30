/// Epoch nanos -> epoch seconds string. The querier parses `start` and `end` as
/// seconds, and allows a fractional part.
pub(crate) fn ns_to_seconds(ns: i64) -> String {
    let negative = ns < 0;
    let ns = ns.unsigned_abs();
    let seconds = ns / 1_000_000_000;
    let nanos = ns % 1_000_000_000;
    let s = if nanos == 0 {
        seconds.to_string()
    } else {
        let mut frac = format!("{nanos:09}");
        while frac.ends_with('0') {
            frac.pop();
        }
        format!("{seconds}.{frac}")
    };
    if negative { format!("-{s}") } else { s }
}
