use super::*;

#[tokio::test]
pub(crate) async fn a_query_over_present_blocks_raises_no_warning() {
    let mut block_store = MemoryBlockStoreFixture::build().writer();

    let first_series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let second_series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "db"),
    ]);
    write_float_block(
        &mut block_store,
        "metrics/float/0001.parquet",
        &first_series,
        1_000,
        1.0,
    )
    .await;
    write_float_block(
        &mut block_store,
        "metrics/float/0002.parquet",
        &second_series,
        1_000,
        2.0,
    )
    .await;

    let InstantUpQuery {
        samples,
        annotations,
    } = instant_up_query(block_store).await;
    check!(samples.len() == 2);
    check!(annotations == Annotations::new());
}
