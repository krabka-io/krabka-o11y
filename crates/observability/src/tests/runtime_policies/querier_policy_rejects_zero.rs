use super::*;

#[test]
pub(crate) fn querier_policy_rejects_zero() {
    for argument in [
        "--querier-frontier-refresh-interval=0s",
        "--querier-dynamic-index-cache-ttl=0s",
        "--querier-shard-index-cache-ttl=0s",
        "--querier-shard-fetch-concurrency=0",
        "--querier-cold-block-fetch-concurrency=0",
        "--querier-hot-tail-bucket-width=0s",
        "--querier-hot-tail-interval=0s",
        "--querier-dependency-reconnect-interval=0s",
    ] {
        check!(
            ServiceConfig::try_parse_from(["krabka-observability", "--target=querier", argument,])
                .is_err(),
            "accepted {argument}"
        );
    }
}
