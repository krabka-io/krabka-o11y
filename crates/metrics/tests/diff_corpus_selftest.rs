#[path = "support/diff_corpus.rs"]
mod diff_corpus;

use assert2::{assert, check};
use diff_corpus::*;
use serde_json::json;

#[test]
fn normalize_is_order_and_epsilon_insensitive() {
    let a = json!({"status":"success","data":{"resultType":"vector","result":[
        {"metric":{"__name__":"x","a":"1"},"value":[0.0,"1.0000001"]},
        {"metric":{"__name__":"x","a":"2"},"value":[0.0,"2.0"]}
    ]}});
    let b = json!({"status":"success","data":{"resultType":"vector","result":[
        {"metric":{"__name__":"x","a":"2"},"value":[0.0,"2.0"]},
        {"metric":{"__name__":"x","a":"1"},"value":[0.0,"1.0"]}
    ]}});

    check!(normalize(&a) == normalize(&b));
    assert_query_equal("order-and-epsilon", &a, &b);
}

#[test]
fn real_value_difference_is_detected() {
    let a = json!({"status":"success","data":{"resultType":"vector","result":[
        {"metric":{"__name__":"x"},"value":[0.0,"1.0"]}
    ]}});
    let b = json!({"status":"success","data":{"resultType":"vector","result":[
        {"metric":{"__name__":"x"},"value":[0.0,"5.0"]}
    ]}});

    check!(normalize(&a) != normalize(&b));
}

#[test]
fn the_hand_written_seed_dataset_is_nonempty() {
    // This dataset is no longer the differential corpus -- that is now read
    // from the vendored `.test` files, in
    // `crates/metrics-service/tests/support/promql_corpus.rs`. It stays because
    // `grafana_integration` asserts panel values against it.
    assert!(!seed_dataset().is_empty());
}
