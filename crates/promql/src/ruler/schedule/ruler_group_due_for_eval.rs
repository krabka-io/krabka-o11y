use super::*;

pub(crate) fn ruler_group_due_for_eval(
    tenant: &str,
    namespace: &str,
    group_name: &str,
    group: &serde_yaml::Value,
    group_state: &RulerGroupState,
    eval_time_ms: i64,
) -> bool {
    let Some(last_eval_ms) = group_state.last_eval_ms(tenant, namespace, group_name) else {
        return true;
    };
    // A malformed `interval` is a config error; skip the group rather than
    // treating an unparseable value as `0` and re-evaluating every tick. The
    // `for`/`expr` paths surface the same parse error as a hard failure.
    let Ok(interval) = yaml_duration(group, "interval") else {
        return false;
    };
    eval_time_ms.saturating_sub(last_eval_ms) >= interval.millis_i64()
}
