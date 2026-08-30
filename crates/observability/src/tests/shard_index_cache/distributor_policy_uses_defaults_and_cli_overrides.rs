use super::*;

#[test]
pub(crate) fn distributor_policy_uses_defaults_and_cli_overrides() {
    let defaults = ServiceConfig::parse_from(["krabka-observability", "--target", "distributor"]);
    check!(defaults.reject_old_samples_max_age == days(7));
    check!(defaults.creation_grace_period == minutes(10));
    check!(defaults.ingest_quota_burst_window == secs(1));
    check!(defaults.wal_connect_startup_deadline == minutes(2));
    check!(defaults.wal_connect_attempt_timeout == secs(15));
    check!(defaults.wal_connect_initial_backoff == millis(200));
    check!(defaults.wal_connect_max_backoff == secs(2));

    let configured = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target",
        "distributor",
        "--reject-old-samples-max-age=8d",
        "--creation-grace-period=11m",
        "--ingest-quota-burst-window=2s",
        "--wal-connect-startup-deadline=3m",
        "--wal-connect-attempt-timeout=16s",
        "--wal-connect-initial-backoff=300ms",
        "--wal-connect-max-backoff=3s",
    ])
    .expect("valid distributor policy");
    check!(configured.reject_old_samples_max_age == days(8));
    check!(configured.creation_grace_period == minutes(11));
    check!(configured.ingest_quota_burst_window == secs(2));
    check!(configured.wal_connect_startup_deadline == minutes(3));
    check!(configured.wal_connect_attempt_timeout == secs(16));
    check!(configured.wal_connect_initial_backoff == millis(300));
    check!(configured.wal_connect_max_backoff == secs(3));
}
