use super::*;

#[test]
pub(crate) fn compactor_policy_uses_defaults_and_cli_overrides() {
    let defaults = ServiceConfig::default();
    check!(defaults.compactor_wal_poll_timeout == millis(500));
    check!(defaults.compactor_accumulation_window == secs(2));
    check!(defaults.compactor_accumulation_poll_timeout == millis(250));
    check!(defaults.compactor_max_records_per_batch.get() == 4096);
    check!(defaults.compactor_idle_interval == millis(10));
    check!(defaults.compactor_object_store_initial_backoff == millis(10));
    check!(defaults.compactor_object_store_max_backoff == millis(500));

    let configured = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target=compactor",
        "--compactor-wal-poll-timeout=600ms",
        "--compactor-accumulation-window=3s",
        "--compactor-accumulation-poll-timeout=300ms",
        "--compactor-max-records-per-batch=5000",
        "--compactor-idle-interval=20ms",
        "--compactor-object-store-initial-backoff=20ms",
        "--compactor-object-store-max-backoff=600ms",
    ])
    .expect("valid compactor policy");
    check!(configured.compactor_wal_poll_timeout == millis(600));
    check!(configured.compactor_accumulation_window == secs(3));
    check!(configured.compactor_accumulation_poll_timeout == millis(300));
    check!(configured.compactor_max_records_per_batch.get() == 5000);
    check!(configured.compactor_idle_interval == millis(20));
    check!(configured.compactor_object_store_initial_backoff == millis(20));
    check!(configured.compactor_object_store_max_backoff == millis(600));
}
