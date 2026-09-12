use assert2::{assert, check};

use super::*;

#[test]
fn ranges_are_aligned_to_absolute_split_boundaries() {
    let ranges = split_ranges(
        TimeRange {
            start_ns: 7,
            end_ns: 35,
        },
        10,
    );

    assert!(
        ranges
            == vec![
                TimeRange {
                    start_ns: 7,
                    end_ns: 10,
                },
                TimeRange {
                    start_ns: 10,
                    end_ns: 20,
                },
                TimeRange {
                    start_ns: 20,
                    end_ns: 30,
                },
                TimeRange {
                    start_ns: 30,
                    end_ns: 35,
                },
            ]
    );
}

#[test]
fn empty_shard_plan_covers_the_whole_fingerprint_space() {
    let shards = fingerprint_shards(&BTreeSet::new(), &[], 1);

    assert!(shards == vec![full_fingerprint_bounds()]);
}

#[test]
fn stream_merge_groups_labels_deduplicates_and_applies_one_global_limit() {
    let first = json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {"stream":{"app":"a"},"values":[["10","a10"],["30","a30"]]},
            ],
            "stats": {"summary":{"totalBytesProcessed":2,"queueTime":0.1}}
        },
        "warnings": ["one"]
    });
    let second = json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {"stream":{"app":"a"},"values":[["10","a10"],["20","a20"]]},
                {"stream":{"app":"b"},"values":[["25","b25"]]},
            ],
            "stats": {"summary":{"totalBytesProcessed":3,"queueTime":0.3}}
        },
        "warnings": ["one", "two"]
    });

    let merged = merge_frontend_results(
        vec![first, second],
        LokiDirection::Backward,
        Some(3),
        None,
        40,
    );

    check!(merged["data"]["result"].as_array().map(Vec::len) == Some(2));
    check!(merged["data"]["result"][0]["values"] == json!([["30", "a30"], ["20", "a20"]]));
    check!(merged["data"]["result"][1]["values"] == json!([["25", "b25"]]));
    check!(merged["data"]["stats"]["summary"]["totalBytesProcessed"] == json!(5));
    check!(merged["data"]["stats"]["summary"]["queueTime"] == json!(0.3));
    assert!(merged["warnings"] == json!(["one", "two"]));
}

#[test]
fn matrix_merge_groups_labels_orders_samples_and_deduplicates_boundaries() {
    let first = json!({
        "status":"success",
        "data":{"resultType":"matrix","result":[
            {"metric":{"app":"a"},"values":[[2.0,"2"],[3.0,"3"]]}
        ]}
    });
    let second = json!({
        "status":"success",
        "data":{"resultType":"matrix","result":[
            {"metric":{"app":"a"},"values":[[1.0,"1"],[2.0,"2"]]}
        ]}
    });

    let merged = merge_frontend_results(
        vec![first, second],
        LokiDirection::Forward,
        None,
        None,
        i64::MAX,
    );

    assert!(
        merged["data"]["result"]
            == json!([{"metric":{"app":"a"},"values":[[1.0,"1"],[2.0,"2"],[3.0,"3"]]}])
    );
}

fn query_params(direction: &str) -> QueryParams {
    QueryParams {
        query: "{app=\"api\"}".to_string(),
        time: None,
        start: Some(0),
        end: Some(10),
        since: None,
        step: None,
        interval: Some(2),
        limit: Some(1),
        direction: Some(direction.to_string()),
        delay_for: None,
    }
}

#[test]
fn partitioned_queries_keep_direction_and_limit() {
    let params = planned_query_params(
        &query_params("backward"),
        TimeRange {
            start_ns: 0,
            end_ns: 5,
        },
        true,
    );

    assert!(params.direction.as_deref() == Some("backward"));
    assert!(params.limit == Some(1));
    assert!(params.interval.is_none());
}

#[test]
fn log_cache_keys_include_direction() {
    let range = TimeRange {
        start_ns: 0,
        end_ns: 10,
    };
    let bounds = full_fingerprint_bounds();
    let forward = logs_cache_key(
        "tenant-a",
        &query_params("forward"),
        range,
        bounds,
        LokiStreamEncoding::default(),
    );
    let backward = logs_cache_key(
        "tenant-a",
        &query_params("backward"),
        range,
        bounds,
        LokiStreamEncoding::default(),
    );

    assert!(forward != backward);
}

#[test]
fn configured_delete_requests_disable_log_result_caching() {
    let result = json!({"data":{"result":[{"stream":{"app":"api"},"values":[]}]}});

    assert!(logs_result_is_cacheable(&result, false));
    assert!(!logs_result_is_cacheable(&result, true));
}
