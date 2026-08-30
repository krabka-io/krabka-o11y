use super::*;

#[test]
pub(crate) fn ruler_group_state_replays_compacted_last_eval_records() {
    let mut state = super::super::RulerGroupState::default();
    state.apply_records(vec![
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "availability".to_string(),
            last_eval_ms: 60_000,
        },
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-b".to_string(),
            group: "latency".to_string(),
            last_eval_ms: 90_000,
        },
        super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "availability".to_string(),
            last_eval_ms: 120_000,
        },
    ]);

    for (tenant, namespace, group, want) in [
        ("tenant-a", "team-a", "availability", Some(120_000)),
        ("tenant-a", "team-b", "latency", Some(90_000)),
        ("tenant-b", "team-a", "availability", None),
    ] {
        assert2::assert!(state.last_eval_ms(tenant, namespace, group) == want);
    }
}
