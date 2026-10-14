use super::*;

/// Evaluating one rule prunes the alert states belonging to *it*, and
/// leaves every other rule's alone. The three identity fields are or-ed,
/// so a state is kept when any one of them differs -- and each has to be
/// the only difference, or a mutant that requires two of them would still
/// keep it.
#[test]
pub(crate) fn evaluating_one_alert_rule_leaves_the_other_rules_states_alone() {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct AlertIdentity {
        tenant: String,
        alert_name: String,
        query: String,
    }

    let states = super::super::prelude::SharedPrometheusAlertStates::default();
    let fields: serde_yaml::Mapping =
        serde_yaml::from_str("severity: page\n").expect("the rule fields parse");
    let result = serde_json::json!({
        "data": { "result": [{ "metric": {"job": "api"}, "value": [0, "1"] }] }
    });
    let evaluate = |tenant: &str, alert: &str, query: &str| {
        super::super::prelude::prometheus_alerts_from_query_result(
            &states, tenant, alert, &fields, query, 1_000, &result,
        );
    };
    let held = || {
        states
            .alerts
            .lock()
            .expect("the alert states lock is not poisoned")
            .keys()
            .map(|key| AlertIdentity {
                tenant: key.tenant.clone(),
                alert_name: key.alert_name.clone(),
                query: key.query.clone(),
            })
            .collect::<Vec<_>>()
    };

    evaluate("t1", "A", "up");
    check!(held().len() == 1);

    // Each of these differs from the first in exactly one field, and none
    // of them may disturb it.
    evaluate("t2", "A", "up");
    evaluate("t1", "B", "up");
    evaluate("t1", "A", "down");

    let mut keys = held();
    keys.sort();
    check!(
        keys == vec![
            AlertIdentity {
                tenant: "t1".to_string(),
                alert_name: "A".to_string(),
                query: "down".to_string()
            },
            AlertIdentity {
                tenant: "t1".to_string(),
                alert_name: "A".to_string(),
                query: "up".to_string()
            },
            AlertIdentity {
                tenant: "t1".to_string(),
                alert_name: "B".to_string(),
                query: "up".to_string()
            },
            AlertIdentity {
                tenant: "t2".to_string(),
                alert_name: "A".to_string(),
                query: "up".to_string()
            },
        ],
        "got {keys:?}"
    );
}
