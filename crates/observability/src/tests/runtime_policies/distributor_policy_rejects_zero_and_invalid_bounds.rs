use super::*;

#[test]
pub(crate) fn distributor_policy_rejects_zero_and_invalid_bounds() {
    for argument in [
        "--reject-old-samples-max-age=0s",
        "--creation-grace-period=0s",
        "--ingest-quota-burst-window=0s",
        "--wal-connect-startup-deadline=0s",
        "--wal-connect-attempt-timeout=0s",
        "--wal-connect-initial-backoff=0s",
        "--wal-connect-max-backoff=0s",
    ] {
        check!(
            ServiceConfig::try_parse_from([
                "krabka-observability",
                "--target",
                "distributor",
                argument,
            ])
            .is_err(),
            "accepted {argument}"
        );
    }

    let attempt_above_deadline = ServiceConfig::parse_from([
        "krabka-observability",
        "--target",
        "distributor",
        "--wal-connect-startup-deadline=1s",
        "--wal-connect-attempt-timeout=2s",
    ]);
    check!(validate_distributor_policy(&attempt_above_deadline).is_err());

    let initial_above_max = ServiceConfig::parse_from([
        "krabka-observability",
        "--target",
        "distributor",
        "--wal-connect-initial-backoff=2s",
        "--wal-connect-max-backoff=1s",
    ]);
    check!(validate_distributor_policy(&initial_above_max).is_err());

    // Equal is not "exceeds". Both cases above are rejections, so the
    // comparisons could have refused a timeout that merely *matches* its
    // deadline and nothing would have noticed.
    for (deadline, timeout) in [
        (
            "--wal-connect-startup-deadline=1s",
            "--wal-connect-attempt-timeout=1s",
        ),
        (
            "--wal-connect-initial-backoff=1s",
            "--wal-connect-max-backoff=1s",
        ),
    ] {
        let at_the_limit = ServiceConfig::parse_from([
            "krabka-observability",
            "--target",
            "distributor",
            deadline,
            timeout,
        ]);
        check!(
            validate_distributor_policy(&at_the_limit).is_ok(),
            "{deadline} with {timeout}"
        );
    }
}
