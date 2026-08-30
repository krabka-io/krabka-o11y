use super::*;

pub(crate) fn intern_string(strings: &mut Vec<String>, ids: &mut BTreeMap<String, i64>, value: &str) -> i64 {
    if let Some(id) = ids.get(value) {
        return *id;
    }
    let id = i64::try_from(strings.len()).expect("string table index fits i64");
    strings.push(value.to_string());
    ids.insert(value.to_string(), id);
    id
}
