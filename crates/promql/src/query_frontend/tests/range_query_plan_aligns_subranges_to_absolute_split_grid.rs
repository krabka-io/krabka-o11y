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
