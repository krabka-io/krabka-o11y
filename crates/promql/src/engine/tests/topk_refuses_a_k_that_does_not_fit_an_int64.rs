use super::*;

/// Prometheus bounds `k` to what an `int64` holds: `topk` refuses any parameter
/// greater than or equal to `maxInt64`, which is `2^63 - 1024` and the last
/// `f64` below `2^63`. A `usize` does not impose that bound on a 64-bit target
/// -- it reaches almost twice as far -- so the bound has to be spelled out.
#[tokio::test]
pub(crate) async fn topk_refuses_a_k_that_does_not_fit_an_int64() {
    let engine = memory_bytes_engine();

    // `2^63` and the bound itself are both refused; `1e19` is the value that a
    // `u64` would have swallowed whole.
    for query in [
        "topk(1e19, memory_bytes)",
        "topk(9223372036854774784, memory_bytes)",
    ] {
        let error = engine
            .query_instant(&tenant_id("tenant-a"), query, 10_000)
            .await
            .unwrap_err();

        check!(matches!(error, PromqlError::Plan(_)), "{query}");
        check!(format!("{error}").contains("overflows int64"), "{query}");
    }

    // The next `f64` below the bound is `2^63 - 2048`, which is accepted and
    // selects everything.
    let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "topk(9223372036854773760, memory_bytes)",
            10_000,
        )
        .await
        .expect("a k just inside the int64 bound");
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected a vector");
    };
    let projection = topk_projection(&samples);
    assert2::assert!(projection.len() == 3);
    assert2::assert!(projection[0].instance == Some("a"));
    assert2::assert!(projection[0].value == 1.0);
    assert2::assert!(projection[1].instance == Some("b"));
    assert2::assert!(projection[1].value == 3.0);
    assert2::assert!(projection[2].instance == Some("c"));
    assert2::assert!(projection[2].value == 2.0);
}
