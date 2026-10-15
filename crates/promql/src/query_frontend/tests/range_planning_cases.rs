use super::*;

mod range_query_plan_aligns_subranges_to_absolute_split_grid {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct RangeBoundsCase {
        start_ms: i64,
        end_ms: i64,
    }

    #[test]
    pub(crate) fn range_query_plan_aligns_subranges_to_absolute_split_grid() {
        // A window that does not start on a split-interval multiple still
        // produces sub-ranges whose interior boundaries sit on the absolute
        // grid (multiples of split_interval), not relative to start_ms.
        let plan = plan_range_query(
            "up",
            60_000,
            300_000,
            millis(60_000),
            QueryFrontendOptions {
                split_interval: millis(120_000),
                shard_count: 1,
            },
        )
        .unwrap();

        let ranges = plan
            .iter()
            .map(|subquery| RangeBoundsCase {
                start_ms: subquery.start_ms,
                end_ms: subquery.end_ms,
            })
            .collect::<Vec<_>>();

        // Eval points 60k | 120k,180k | 240k,300k bucket into [0,120k),
        // [120k,240k), [240k,360k).
        assert2::assert!(
            ranges
                == vec![
                    RangeBoundsCase {
                        start_ms: 60_000,
                        end_ms: 60_000,
                    },
                    RangeBoundsCase {
                        start_ms: 120_000,
                        end_ms: 180_000,
                    },
                    RangeBoundsCase {
                        start_ms: 240_000,
                        end_ms: 300_000,
                    },
                ]
        );
    }
}

mod range_query_plan_allows_resolution_at_point_cap_boundary {
    use super::*;

    #[test]
    pub(crate) fn range_query_plan_allows_resolution_at_point_cap_boundary() {
        // (end-start)/step = 11_000 / 1 = 11_000 is allowed (cap is exclusive,
        // matching Prometheus's `> 11000`).
        let plan = plan_range_query(
            "up",
            0,
            11_000,
            millis(1),
            QueryFrontendOptions {
                split_interval: millis(120_000),
                shard_count: 1,
            },
        )
        .unwrap();
        assert2::assert!(!plan.is_empty());
    }
}

mod range_query_plan_expands_each_split_across_mimir_query_shards {
    use super::*;

    #[test]
    pub(crate) fn range_query_plan_expands_each_split_across_mimir_query_shards() {
        let plan = plan_range_query(
            "sum(rate(http_requests_total[5m]))",
            0,
            60_000,
            millis(60_000),
            QueryFrontendOptions {
                split_interval: millis(120_000),
                shard_count: 3,
            },
        )
        .unwrap();

        let shard_values = plan
            .iter()
            .map(|subquery| subquery.shard_matcher().expect("sharded subquery").value)
            .collect::<Vec<_>>();

        assert2::assert!(shard_values == vec!["1_of_3", "2_of_3", "3_of_3"]);
        assert2::assert!(
            plan.iter()
                .all(|subquery| subquery.start_ms == 0 && subquery.end_ms == 60_000)
        );

        let matcher = plan[0].shard_matcher().expect("first shard matcher");
        assert2::assert!(matcher.name.as_str() == "__query_shard__");
        assert2::assert!(matcher.op == MatchOp::Eq);
    }
}

mod range_query_plan_rejects_resolution_over_point_cap {
    use super::*;

    #[test]
    pub(crate) fn range_query_plan_rejects_resolution_over_point_cap() {
        // (end-start)/step = 20_000 / 1 = 20_000 > 11_000: the frontend planner
        // must reject before expanding into ~20k per-step sub-queries, matching
        // Prometheus's unconditional resolution front-gate.
        let error = plan_range_query(
            "up",
            0,
            20_000,
            millis(1),
            QueryFrontendOptions {
                split_interval: millis(120_000),
                shard_count: 1,
            },
        )
        .unwrap_err();

        match error {
            PromqlError::Plan(message) => assert2::assert!(
                message
                    == "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
            ),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }
}

mod range_query_plan_shards_every_shardable_aggregate {
    use super::*;

    #[test]
    pub(crate) fn range_query_plan_shards_every_shardable_aggregate() {
        for query in [
            // `avg` reduces from partial `sum` and `count`.
            "avg(up)",
            // `group` reduces with its own aggregate reducer.
            "group(up)",
            // `min` and `max` reduce with their own aggregate reducers.
            "min(up)",
            "max(up)",
            // `stddev` and `stdvar` reduce from moment partials.
            "stddev(up)",
            "stdvar(up)",
            // `topk` and `bottomk` reduce by a final rank over the candidates.
            "topk(2, up)",
            "bottomk(2, up)",
        ] {
            let plan = plan_range_query(
                query,
                0,
                60_000,
                millis(60_000),
                QueryFrontendOptions {
                    split_interval: millis(120_000),
                    shard_count: 3,
                },
            )
            .unwrap();

            assert2::assert!(plan.len() == 3, "{query}");
            assert2::assert!(
                plan.iter().all(|subquery| subquery.shard.is_some()),
                "{query}"
            );
        }
    }
}

mod range_query_plan_splits_on_step_grid_without_duplicate_steps {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    struct PlannedRangeCase {
        start_ms: i64,
        end_ms: i64,
        shard: Option<QueryShard>,
    }

    #[test]
    pub(crate) fn range_query_plan_splits_on_step_grid_without_duplicate_steps() {
        let plan = plan_range_query(
            "rate(http_requests_total[5m])",
            0,
            250_000,
            millis(60_000),
            QueryFrontendOptions {
                split_interval: millis(120_000),
                shard_count: 1,
            },
        )
        .unwrap();

        let ranges = plan
            .iter()
            .map(|subquery| PlannedRangeCase {
                start_ms: subquery.start_ms,
                end_ms: subquery.end_ms,
                shard: subquery.shard,
            })
            .collect::<Vec<_>>();

        // Eval points 0, 60k, 120k, 180k, 240k bucket into absolute
        // split-interval windows [0,120k), [120k,240k), [240k,360k); each
        // sub-range spans the eval points landing in its absolute window with
        // no duplicate step across sub-ranges.
        assert2::assert!(
            ranges
                == vec![
                    PlannedRangeCase {
                        start_ms: 0,
                        end_ms: 60_000,
                        shard: None,
                    },
                    PlannedRangeCase {
                        start_ms: 120_000,
                        end_ms: 180_000,
                        shard: None,
                    },
                    PlannedRangeCase {
                        start_ms: 240_000,
                        end_ms: 240_000,
                        shard: None,
                    },
                ]
        );
    }
}
