use super::{Bytes, RulerStateWalRecord};

#[must_use]
/// Returns the stable broker compaction key for one ruler group or alert.
pub fn ruler_state_compaction_key(record: &RulerStateWalRecord) -> Bytes {
    match record {
        RulerStateWalRecord::Group(record) => Bytes::from(format!(
            "group\0{}\0{}\0{}",
            record.tenant, record.namespace, record.group
        )),
        RulerStateWalRecord::Alert(record) => {
            if record.labels.has_byte_values() {
                let mut bytes = b"alert-bytes\0".to_vec();
                for field in [record.tenant.as_bytes(), record.rule_id.as_bytes()] {
                    bytes.extend_from_slice(&(field.len() as u64).to_le_bytes());
                    bytes.extend_from_slice(field);
                }
                bytes.extend_from_slice(&record.labels.byte_key());
                return Bytes::from(bytes);
            }
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
