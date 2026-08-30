use super::*;

/// Last-evaluation state for ruler groups, rebuildable from compacted records.
#[derive(Debug, Default)]
pub struct RulerGroupState {
    pub(crate) last_eval_ms: BTreeMap<RulerGroupStateKey, i64>,
}

impl RulerGroupState {
    /// Applies one compacted group-state record to the in-memory group tracker.
    pub fn apply_record(&mut self, record: RulerGroupStateRecord) {
        self.last_eval_ms.insert(
            RulerGroupStateKey {
                tenant: record.tenant,
                namespace: record.namespace,
                group: record.group,
            },
            record.last_eval_ms,
        );
    }

    /// Rebuilds group state from compacted group-state records.
    pub fn apply_records<I>(&mut self, records: I)
    where
        I: IntoIterator<Item = RulerGroupStateRecord>,
    {
        for record in records {
            self.apply_record(record);
        }
    }

    #[must_use]
    pub fn last_eval_ms(&self, tenant: &str, namespace: &str, group: &str) -> Option<i64> {
        self.last_eval_ms
            .get(&RulerGroupStateKey {
                tenant: tenant.to_string(),
                namespace: namespace.to_string(),
                group: group.to_string(),
            })
            .copied()
    }
}
