use super::{MatchCmp, MatchValue};

/// Whether `value` satisfies `op` against a string `expected`.
///
/// A regex must match the whole value; an invalid regex matches nothing.
#[must_use]
pub fn string_matches(value: &str, op: MatchCmp, expected: &MatchValue) -> bool {
    let MatchValue::Str(expected) = expected else {
        return false;
    };
    match op {
        MatchCmp::Eq => value == expected,
        MatchCmp::Neq => value != expected,
        MatchCmp::Re => {
            regex::Regex::new(&format!("^(?:{expected})$")).is_ok_and(|re| re.is_match(value))
        }
        MatchCmp::Nre => {
            regex::Regex::new(&format!("^(?:{expected})$")).is_ok_and(|re| !re.is_match(value))
        }
        MatchCmp::Lt | MatchCmp::Lte | MatchCmp::Gt | MatchCmp::Gte => false,
    }
}
