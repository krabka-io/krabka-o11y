use std::collections::BTreeSet;

use assert2::{assert, check};
use krabka_blockstore::{
    BlockDescriptor, BlockKey, LabelIndex, LabelPair, LogBlockIndex as BlockIndex, TimeRange,
    labels,
};
use krabka_logql::{LineFilterOp, PipelineStage, parse_query, plan_stream_query};

fn plan_for_tenant(
    query: &str,
    labels: &LabelIndex,
    blocks: &BlockIndex,
) -> krabka_logql::StreamPlan {
    plan_stream_query(
        "tenant-a",
        TimeRange::new(1, 30).unwrap(),
        parse_query(query).unwrap(),
        labels,
        blocks,
    )
    .unwrap()
}

fn add_test_block(
    blocks: &mut BlockIndex,
    partition: i32,
    first_offset: i64,
    last_offset: i64,
    time: TimeRange,
    fingerprints: impl IntoIterator<Item = u64>,
) {
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", partition, first_offset, last_offset, time),
        fingerprints.into_iter().collect(),
    ));
}

fn block_index_for_series(fingerprints: impl IntoIterator<Item = u64>) -> BlockIndex {
    let mut blocks = BlockIndex::default();
    add_test_block(
        &mut blocks,
        0,
        1,
        10,
        TimeRange::new(10, 20).unwrap(),
        fingerprints,
    );
    blocks
}

#[test]
fn stream_planner_prunes_series_and_blocks_before_line_filters() {
    let mut labels_index = LabelIndex::default();
    let api_prod = labels_index.insert_series(
        "tenant-a",
        labels([LabelPair::new("app", "api"), LabelPair::new("env", "prod")]),
    );
    let api_dev = labels_index.insert_series(
        "tenant-a",
        labels([LabelPair::new("app", "api"), LabelPair::new("env", "dev")]),
    );
    let worker_prod = labels_index.insert_series(
        "tenant-a",
        labels([
            LabelPair::new("app", "worker"),
            LabelPair::new("env", "prod"),
        ]),
    );
    labels_index.insert_series(
        "tenant-b",
        labels([LabelPair::new("app", "api"), LabelPair::new("env", "prod")]),
    );

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([api_prod]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(200, 299).unwrap()),
        BTreeSet::from([api_dev]),
    ));
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 1, 30, 39, TimeRange::new(100, 199).unwrap()),
        BTreeSet::from([worker_prod]),
    ));

    let query = parse_query(r#"{app="api", env!="dev"} |= "error""#).unwrap();
    let plan = plan_stream_query(
        "tenant-a",
        TimeRange::new(150, 250).unwrap(),
        query,
        &labels_index,
        &blocks,
    )
    .unwrap();

    check!(plan.fingerprints == BTreeSet::from([api_prod]));
    check!(
        plan.blocks
            .iter()
            .map(|block| block.key.object_key())
            .collect::<Vec<_>>()
            == ["tenant=tenant-a/partition=0/offsets=10-19/time=100-199.parquet"]
    );
    assert!(matches!(
        &plan.query.pipeline[..],
        [PipelineStage::LineFilter(filter)] if filter.op == LineFilterOp::Contains && filter.pattern == "error"
    ));
}

#[test]
fn stream_planner_keeps_regex_and_negative_matchers_in_index_filter() {
    let mut labels_index = LabelIndex::default();
    let api_prod = labels_index.insert_series(
        "tenant-a",
        labels([LabelPair::new("app", "api"), LabelPair::new("env", "prod")]),
    );
    let web_prod = labels_index.insert_series(
        "tenant-a",
        labels([LabelPair::new("app", "web"), LabelPair::new("env", "prod")]),
    );
    let admin_prod = labels_index.insert_series(
        "tenant-a",
        labels([
            LabelPair::new("app", "admin"),
            LabelPair::new("env", "prod"),
        ]),
    );

    let blocks = block_index_for_series([api_prod, web_prod, admin_prod]);

    let plan = plan_for_tenant(
        r#"{app=~"api|web|admin", app!~"admin"} !~ "debug""#,
        &labels_index,
        &blocks,
    );

    assert!(plan.fingerprints == BTreeSet::from([api_prod, web_prod]));
    assert!(plan.blocks.len() == 1);
}

#[test]
fn stream_planner_treats_empty_compatible_regex_matcher_as_matching_absent_label() {
    let mut labels_index = LabelIndex::default();
    let api_without_env =
        labels_index.insert_series("tenant-a", labels([LabelPair::new("app", "api")]));
    let api_prod = labels_index.insert_series(
        "tenant-a",
        labels([LabelPair::new("app", "api"), LabelPair::new("env", "prod")]),
    );
    let worker_prod = labels_index.insert_series(
        "tenant-a",
        labels([
            LabelPair::new("app", "worker"),
            LabelPair::new("env", "prod"),
        ]),
    );

    let blocks = block_index_for_series([api_without_env, api_prod, worker_prod]);

    let plan = plan_for_tenant(r#"{app="api", env=~".*"}"#, &labels_index, &blocks);

    assert!(plan.fingerprints == BTreeSet::from([api_without_env, api_prod]));
    assert!(plan.blocks.len() == 1);
}

#[test]
fn stream_planner_anchors_regex_label_matchers() {
    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series("tenant-a", labels([LabelPair::new("app", "api")]));
    let worker = labels_index.insert_series("tenant-a", labels([LabelPair::new("app", "worker")]));
    let prefixed_api =
        labels_index.insert_series("tenant-a", labels([LabelPair::new("app", "myapi")]));
    let suffixed_api =
        labels_index.insert_series("tenant-a", labels([LabelPair::new("app", "api-v2")]));

    let mut blocks = BlockIndex::default();
    blocks.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 1, 10, TimeRange::new(10, 20).unwrap()),
        BTreeSet::from([api, worker, prefixed_api, suffixed_api]),
    ));

    let plan = plan_for_tenant(r#"{app=~"api|worker"}"#, &labels_index, &blocks);

    assert!(plan.fingerprints == BTreeSet::from([api, worker]));
    assert!(plan.blocks.len() == 1);
}
