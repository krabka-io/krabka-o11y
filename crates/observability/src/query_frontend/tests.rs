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
            "stats": {"summary":{"totalBytesProcessed":2}}
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
            "stats": {"summary":{"totalBytesProcessed":3}}
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
