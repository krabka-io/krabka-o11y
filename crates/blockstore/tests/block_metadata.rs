use std::collections::BTreeSet;

use assert2::assert;
use krabka_blockstore::{BlockLevel, BlockMeta, BlockTimeBounds, Index};

fn block(
    key: &str,
    window: BlockTimeBounds,
    row_count: usize,
    fingerprints: &[u64],
    level: u32,
) -> BlockMeta {
    BlockMeta {
        tenant: "t".into(),
        object_key: key.into(),
        min_ts: window.min_ts,
        max_ts: window.max_ts,
        row_count,
        fingerprints: fingerprints.to_vec(),
        level: BlockLevel(level),
    }
}

#[test]
fn block_metadata_preserves_full_records_through_restatement_and_retirement() {
    let mut index = Index::new();
    for meta in [
        block(
            "late",
            BlockTimeBounds {
                min_ts: 20,
                max_ts: 30,
            },
            3,
            &[9, 2, 9],
            2,
        ),
        block(
            "early",
            BlockTimeBounds {
                min_ts: -10,
                max_ts: 0,
            },
            2,
            &[7],
            0,
        ),
        block(
            "empty",
            BlockTimeBounds {
                min_ts: 20,
                max_ts: 30,
            },
            0,
            &[],
            1,
        ),
        BlockMeta {
            tenant: "other".into(),
            ..block(
                "late",
                BlockTimeBounds {
                    min_ts: 20,
                    max_ts: 30,
                },
                99,
                &[77],
                9,
            )
        },
    ] {
        index.add_block(&meta);
    }
    let original = vec![
        block(
            "early",
            BlockTimeBounds {
                min_ts: -10,
                max_ts: 0,
            },
            2,
            &[7],
            0,
        ),
        block(
            "empty",
            BlockTimeBounds {
                min_ts: 20,
                max_ts: 30,
            },
            0,
            &[],
            1,
        ),
        block(
            "late",
            BlockTimeBounds {
                min_ts: 20,
                max_ts: 30,
            },
            3,
            &[2, 9],
            2,
        ),
    ];
    assert!(index.all_blocks("t") == original);
    let snapshot = index.clone();

    // An unchanged fingerprint set updates the same block and its time order.
    index.add_block(&block(
        "late",
        BlockTimeBounds {
            min_ts: -30,
            max_ts: -20,
        },
        11,
        &[9, 2],
        4,
    ));
    assert!(
        index.all_blocks("t")
            == vec![
                block(
                    "late",
                    BlockTimeBounds {
                        min_ts: -30,
                        max_ts: -20
                    },
                    11,
                    &[2, 9],
                    4
                ),
                block(
                    "early",
                    BlockTimeBounds {
                        min_ts: -10,
                        max_ts: 0
                    },
                    2,
                    &[7],
                    0
                ),
                block(
                    "empty",
                    BlockTimeBounds {
                        min_ts: 20,
                        max_ts: 30
                    },
                    0,
                    &[],
                    1
                ),
            ]
    );

    // A changed set retires the prior block record and its postings.
    index.add_block(&block(
        "early",
        BlockTimeBounds {
            min_ts: 10,
            max_ts: 15,
        },
        6,
        &[11, 2, 11],
        5,
    ));
    assert!(
        index.all_blocks("t")
            == vec![
                block(
                    "late",
                    BlockTimeBounds {
                        min_ts: -30,
                        max_ts: -20
                    },
                    11,
                    &[2, 9],
                    4
                ),
                block(
                    "early",
                    BlockTimeBounds {
                        min_ts: 10,
                        max_ts: 15
                    },
                    6,
                    &[2, 11],
                    5
                ),
                block(
                    "empty",
                    BlockTimeBounds {
                        min_ts: 20,
                        max_ts: 30
                    },
                    0,
                    &[],
                    1
                ),
            ]
    );
    assert!(
        index
            .candidate_blocks("t", &BTreeSet::from([7]), i64::MIN, i64::MAX)
            .is_empty()
    );
    assert!(index.remove_blocks("t", &["late".into(), "missing".into()]) == 1);
    assert!(
        index.all_blocks("t")
            == vec![
                block(
                    "early",
                    BlockTimeBounds {
                        min_ts: 10,
                        max_ts: 15
                    },
                    6,
                    &[2, 11],
                    5
                ),
                block(
                    "empty",
                    BlockTimeBounds {
                        min_ts: 20,
                        max_ts: 30
                    },
                    0,
                    &[],
                    1
                ),
            ]
    );

    let compacted = block(
        "compacted",
        BlockTimeBounds {
            min_ts: 0,
            max_ts: 50,
        },
        8,
        &[11, 2, 11],
        6,
    );
    index.replace_blocks("t", &["early".into(), "empty".into()], &[compacted]);
    let expected = vec![block(
        "compacted",
        BlockTimeBounds {
            min_ts: 0,
            max_ts: 50,
        },
        8,
        &[2, 11],
        6,
    )];
    assert!(index.all_blocks("t") == expected);
    let restored: Index = serde_json::from_value(serde_json::to_value(&index).unwrap()).unwrap();
    assert!(restored.all_blocks("t") == expected);
    assert!(snapshot.all_blocks("t") == original);
    assert!(
        index.all_blocks("other")
            == vec![BlockMeta {
                tenant: "other".into(),
                ..block(
                    "late",
                    BlockTimeBounds {
                        min_ts: 20,
                        max_ts: 30
                    },
                    99,
                    &[77],
                    9
                )
            }]
    );
    assert!(index.all_blocks("missing").is_empty());
    assert!(index.remove_blocks("t", &["compacted".into()]) == 1);
    assert!(index.all_blocks("t").is_empty());
    assert!(index.remove_blocks("t", &["compacted".into()]) == 0);
}
