use krabka_blockstore::TimeRange;

use super::{BlockCatalog, BlockMetaInfo, CatalogError, async_trait};

/// A canned block catalog for tests.
pub struct MockCatalog {
    pub(crate) blocks: Vec<BlockMetaInfo>,
}

#[derive(Clone, Copy)]
pub struct MockBlock<'a> {
    pub block_id: &'a str,
    pub time_range: TimeRange,
    pub row_group_bytes: &'a [u64],
}

impl MockCatalog {
    #[must_use]
    pub fn new(blocks: Vec<BlockMetaInfo>) -> Self {
        Self { blocks }
    }

    #[must_use]
    pub fn one_block_with_row_groups(
        block_id: &str,
        time_range: TimeRange,
        row_group_bytes: &[u64],
    ) -> Self {
        Self::new(vec![BlockMetaInfo::with_row_groups(
            block_id,
            time_range,
            row_group_bytes,
        )])
    }

    #[must_use]
    pub fn two_blocks_with_row_groups(first: MockBlock<'_>, second: MockBlock<'_>) -> Self {
        Self::new(vec![
            BlockMetaInfo::with_row_groups(first.block_id, first.time_range, first.row_group_bytes),
            BlockMetaInfo::with_row_groups(
                second.block_id,
                second.time_range,
                second.row_group_bytes,
            ),
        ])
    }

    #[must_use]
    pub fn two_small_blocks() -> Self {
        Self::two_blocks_with_row_groups(
            Self::first_fixture_block(),
            MockBlock {
                block_id: "b2",
                time_range: TimeRange {
                    start_ns: 100,
                    end_ns: 200,
                },
                row_group_bytes: &[500],
            },
        )
    }

    #[must_use]
    pub fn split_row_group_blocks() -> Self {
        Self::two_blocks_with_row_groups(
            Self::first_fixture_block(),
            MockBlock {
                block_id: "b2",
                time_range: TimeRange {
                    start_ns: 100,
                    end_ns: 200,
                },
                row_group_bytes: &[15_000, 15_000],
            },
        )
    }

    fn first_fixture_block() -> MockBlock<'static> {
        MockBlock {
            block_id: "b1",
            time_range: TimeRange {
                start_ns: 0,
                end_ns: 100,
            },
            row_group_bytes: &[500],
        }
    }
}

#[async_trait]
impl BlockCatalog for MockCatalog {
    async fn blocks(
        &self,
        _tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError> {
        Ok(self
            .blocks
            .iter()
            .filter(|b| b.end_ns >= start_ns && b.start_ns <= end_ns)
            .cloned()
            .collect())
    }
}
