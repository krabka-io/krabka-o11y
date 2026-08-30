use super::*;

pub(crate) fn string_matches(value: &str, op: MatchCmp, expected: &MatchValue) -> bool {
    let MatchValue::Str(expected) = expected else {
        return false;
    };
    match op {
        MatchCmp::Eq => value
            .partial_cmp(expected.as_str())
            .is_some_and(std::cmp::Ordering::is_eq),
        MatchCmp::Neq => !value
            .partial_cmp(expected.as_str())
            .is_some_and(std::cmp::Ordering::is_eq),
        MatchCmp::Re => {
            regex::Regex::new(&format!("^(?:{expected})$")).is_ok_and(|re| re.is_match(value))
        }
        MatchCmp::Nre => {
            regex::Regex::new(&format!("^(?:{expected})$")).is_ok_and(|re| !re.is_match(value))
        }
        MatchCmp::Lt | MatchCmp::Lte | MatchCmp::Gt | MatchCmp::Gte => false,
    }
}
