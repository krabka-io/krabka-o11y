use super::*;

pub(crate) fn string_table(dict: &pb::otlp_profiles::ProfilesDictionary) -> Vec<String> {
    if dict.string_table.is_empty() {
        vec![String::new()]
    } else {
        dict.string_table.clone()
    }
}
