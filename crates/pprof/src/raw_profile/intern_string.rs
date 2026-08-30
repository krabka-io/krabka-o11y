use super::HashMap;

pub(crate) fn intern_string(
    strings: &mut Vec<String>,
    index: &mut HashMap<String, i64>,
    value: &str,
) -> i64 {
    if let Some(slot) = index.get(value) {
        return *slot;
    }
    let slot = i64::try_from(strings.len()).expect("string index fits i64");
    strings.push(value.to_string());
    index.insert(value.to_string(), slot);
    slot
}
