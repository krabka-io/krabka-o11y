use super::*;

/// The three per-query limits share a shape: unset means no limit, a query
/// exactly at the limit is allowed, and one unit over is refused. Each is
/// checked at all three points, because `>` and `>=` differ only at the
/// boundary and "unset" is a third answer distinct from a limit of zero.
///
/// They are tested together because they are parallel by design and a
/// reader comparing them should see the same three cases each; a mutant
/// swapping one limit's comparison for another's is caught by their
/// carrying different values.
#[test]
pub(crate) fn every_per_query_limit_admits_exactly_its_boundary() {
    use krabka_blockstore::{BlockDescriptor, BlockKey, TimeRange};
    use krabka_logql::{StreamPlan, StreamQuery};

    let plan = |fingerprints: usize, block_bytes: &[u32]| StreamPlan {
        tenant: "tenant".to_string(),
        time_range: TimeRange::new(0, 10).expect("a valid range"),
        query: StreamQuery {
            matchers: Vec::new(),
            pipeline: Vec::new(),
        },
        fingerprints: (0..u64::try_from(fingerprints).expect("a small count")).collect(),
        blocks: block_bytes
            .iter()
            .enumerate()
            .map(|(index, size)| {
                BlockDescriptor::new_with_size(
                    BlockKey::new(
                        "tenant",
                        0,
                        i64::try_from(index).expect("a small index"),
                        i64::try_from(index).expect("a small index"),
                        TimeRange::new(0, 10).expect("a valid range"),
                    ),
                    BTreeSet::new(),
                    krabka_units::bytes(*size),
                )
            })
            .collect(),
    };
    let base = || {
        super::super::prelude::QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
    };

    // Series: three fingerprints against a limit of three, then two.
    check!(
        super::super::prelude::validate_query_series_limit(&base(), &plan(3, &[])).is_ok(),
        "unset"
    );
    check!(
        super::super::prelude::validate_query_series_limit(
            &base().with_max_query_series(3),
            &plan(3, &[])
        )
        .is_ok(),
        "exactly at the limit"
    );
    check!(
        super::super::prelude::validate_query_series_limit(
            &base().with_max_query_series(2),
            &plan(3, &[])
        )
        .is_err(),
        "one over"
    );

    // Bytes: the planned total is SUMMED across blocks, so two blocks are
    // used -- one block cannot tell a sum from a maximum.
    let two_blocks = plan(0, &[40, 60]);
    check!(
        super::super::prelude::validate_query_bytes_limit(&base(), &two_blocks).is_ok(),
        "unset"
    );
    check!(
        super::super::prelude::validate_query_bytes_limit(
            &base().with_max_query_read(krabka_units::bytes(100)),
            &two_blocks,
        )
        .is_ok(),
        "exactly at the summed limit"
    );
    check!(
        super::super::prelude::validate_query_bytes_limit(
            &base().with_max_query_read(krabka_units::bytes(99)),
            &two_blocks,
        )
        .is_err(),
        "one byte over"
    );

    // Length: measured in bytes of the query text.
    let query = "{app=\"api\"}";
    check!(
        super::super::prelude::validate_query_length_limit(&base(), query).is_ok(),
        "unset"
    );
    check!(
        super::super::prelude::validate_query_length_limit(
            &base().with_max_query_length(krabka_units::bytes(
                u32::try_from(query.len()).expect("a short query")
            )),
            query,
        )
        .is_ok(),
        "exactly at the limit"
    );
    check!(
        super::super::prelude::validate_query_length_limit(
            &base().with_max_query_length(krabka_units::bytes(
                u32::try_from(query.len()).expect("a short query") - 1
            )),
            query,
        )
        .is_err(),
        "one byte over"
    );

    // Each refusal names its own limit rather than a shared message.
    check!(matches!(
        super::super::prelude::validate_query_series_limit(
            &base().with_max_query_series(2),
            &plan(3, &[])
        ),
        Err(HttpQueryError::QuerySeriesTooLarge { .. })
    ));
    check!(matches!(
        super::super::prelude::validate_query_bytes_limit(
            &base().with_max_query_read(krabka_units::bytes(99)),
            &two_blocks,
        ),
        Err(HttpQueryError::QueryBytesTooLarge { .. })
    ));
    check!(matches!(
        super::super::prelude::validate_query_length_limit(
            &base().with_max_query_length(krabka_units::bytes(1)),
            query,
        ),
        Err(HttpQueryError::QueryLengthTooLarge { .. })
    ));
}
