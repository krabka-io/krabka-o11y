pub(crate) fn intern_string(strings: &mut Vec<String>, value: &str) -> i64 {
    if let Some(idx) = strings.iter().position(|existing| existing == value) {
        return i64::try_from(idx).expect("string index fits i64");
    }
    let idx = i64::try_from(strings.len()).expect("string index fits i64");
    strings.push(value.to_string());
    idx
}
