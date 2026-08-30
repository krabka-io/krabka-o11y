use super::{Bytes, RulerStateWalRecord};

#[must_use]
pub fn ruler_state_compaction_key(record: &RulerStateWalRecord) -> Bytes {
    match record {
        RulerStateWalRecord::Group(record) => Bytes::from(format!(
            "group\0{}\0{}\0{}",
            record.tenant, record.namespace, record.group
        )),
        RulerStateWalRecord::Alert(record) => {
            let mut key = format!("alert\0{}\0{}", record.tenant, record.rule_id);
            for (name, value) in &record.labels {
                key.push('\0');
                key.push_str(name);
                key.push('=');
                key.push_str(value);
            }
            Bytes::from(key)
        }
    }
}
