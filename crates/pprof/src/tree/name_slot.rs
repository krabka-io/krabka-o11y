use super::HashMap;

pub(crate) fn name_slot(
    names: &mut Vec<String>,
    index: &mut HashMap<String, i64>,
    name: &str,
) -> i64 {
    if let Some(slot) = index.get(name) {
        return *slot;
    }
    let slot = i64::try_from(names.len()).expect("name index fits i64");
    names.push(name.to_string());
    index.insert(name.to_string(), slot);
    slot
}
