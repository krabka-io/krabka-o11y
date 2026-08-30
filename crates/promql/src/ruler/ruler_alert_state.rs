use super::{AlertStateKey, BTreeMap, RulerAlertStateRecord};

/// Pending/firing alert state for ruler evaluations.
#[derive(Debug, Default)]
pub struct RulerAlertState {
    pub(crate) active_since_ms: BTreeMap<AlertStateKey, i64>,
    /// Wall-clock deadline for each alert instance that has reached the firing
    /// state. The deadline is `eval_time + keep_firing_for`. The instance must
    /// keep firing until that deadline after its series stops matching. An entry
    /// here also marks the instance as fired, so a series that only ever pended
    /// does not emit a resolved alert.
    pub(crate) keep_firing_until_ms: BTreeMap<AlertStateKey, i64>,
}

impl RulerAlertState {
    /// Applies one compacted alert-state record to the in-memory alert tracker.
    pub fn apply_record(&mut self, record: RulerAlertStateRecord) {
        let keep_firing_until_ms = record.keep_firing_until_ms;
        let key = AlertStateKey {
            tenant: record.tenant,
            rule_id: record.rule_id,
            labels: record.labels,
        };
        if let Some(active_since_ms) = record.active_since_ms {
            self.active_since_ms.insert(key.clone(), active_since_ms);
            match keep_firing_until_ms {
                Some(until_ms) => {
                    self.keep_firing_until_ms.insert(key, until_ms);
                }
                None => {
                    self.keep_firing_until_ms.remove(&key);
                }
            }
        } else {
            self.active_since_ms.remove(&key);
            self.keep_firing_until_ms.remove(&key);
        }
    }

    /// Rebuilds alert state from compacted alert-state records.
    pub fn apply_records<I>(&mut self, records: I)
    where
        I: IntoIterator<Item = RulerAlertStateRecord>,
    {
        for record in records {
            self.apply_record(record);
        }
    }
}
