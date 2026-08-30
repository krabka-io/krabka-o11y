use super::*;

#[test]
pub(crate) fn compactor_policy_rejects_zero_and_invalid_bounds() {
    for argument in [
        "--compactor-wal-poll-timeout=0s",
        "--compactor-accumulation-window=0s",
        "--compactor-accumulation-poll-timeout=0s",
        "--compactor-max-records-per-batch=0",
        "--compactor-idle-interval=0s",
        "--compactor-object-store-initial-backoff=0s",
        "--compactor-object-store-max-backoff=0s",
    ] {
        check!(
            ServiceConfig::try_parse_from(
                ["krabka-observability", "--target=compactor", argument,]
            )
            .is_err(),
            "accepted {argument}"
        );
    }

    let poll_above_window = ServiceConfig::parse_from([
        "krabka-observability",
        "--target=compactor",
        "--compactor-accumulation-window=1s",
        "--compactor-accumulation-poll-timeout=2s",
    ]);
    check!(validate_compactor_policy(&poll_above_window).is_err());

    let initial_above_max = ServiceConfig::parse_from([
        "krabka-observability",
        "--target=compactor",
        "--compactor-object-store-initial-backoff=2s",
        "--compactor-object-store-max-backoff=1s",
    ]);
    check!(validate_compactor_policy(&initial_above_max).is_err());

    // And the same pair of boundaries here.
    for (window, timeout) in [
        (
            "--compactor-accumulation-window=1s",
            "--compactor-accumulation-poll-timeout=1s",
        ),
        (
            "--compactor-object-store-initial-backoff=1s",
            "--compactor-object-store-max-backoff=1s",
        ),
    ] {
        let at_the_limit = ServiceConfig::parse_from([
            "krabka-observability",
            "--target=compactor",
            window,
            timeout,
        ]);
        check!(
            validate_compactor_policy(&at_the_limit).is_ok(),
            "{window} with {timeout}"
        );
    }
}
