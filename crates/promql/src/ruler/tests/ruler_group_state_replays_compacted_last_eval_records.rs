#[derive(bon::Builder)]
struct RulerEvalExpectation {
    tenant: &'static str,
    namespace: &'static str,
    group: &'static str,
    expected: Option<i64>,
}

#[test]
pub(crate) fn ruler_group_state_replays_compacted_last_eval_records() {
    let mut state = super::super::RulerGroupState::default();
    state.apply_records(vec![
        super::super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "availability".to_string(),
            last_eval_ms: 60_000,
        },
        super::super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-b".to_string(),
            group: "latency".to_string(),
            last_eval_ms: 90_000,
        },
        super::super::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "availability".to_string(),
            last_eval_ms: 120_000,
        },
    ]);

    for expected in [
        RulerEvalExpectation::builder()
            .tenant("tenant-a")
            .namespace("team-a")
            .group("availability")
            .expected(120_000)
            .build(),
        RulerEvalExpectation::builder()
            .tenant("tenant-a")
            .namespace("team-b")
            .group("latency")
            .expected(90_000)
            .build(),
        RulerEvalExpectation::builder()
            .tenant("tenant-b")
            .namespace("team-a")
            .group("availability")
            .build(),
    ] {
        assert2::assert!(
            state.last_eval_ms(expected.tenant, expected.namespace, expected.group)
                == expected.expected
        );
    }
}
