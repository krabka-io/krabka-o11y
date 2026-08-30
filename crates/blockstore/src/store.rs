//! Query facade over object storage, index pruning, and `DataFusion` scans.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::MemTable,
    prelude::{ParquetReadOptions, SessionContext},
};
use krabka_units::ByteSize;
use object_store::ObjectStore;
use tracing::instrument;
use url::Url;

use crate::{
    error::{BlockStoreError, Result},
    index::Index,
    matcher::LabelMatcher,
    reader::{
        DEFAULT_BLOCK_READ_MAX, RowGroupMeta, read_block_row_groups_with_max_bytes,
        read_row_group_metadata_with_max_bytes,
    },
    writer::BlockWriter,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, LargeStringArray, StringArray, StringViewArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

    use super::*;
    use crate::{
        labels::Labels,
        matcher::{LabelMatcher, MatchOp},
    };

    fn log_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]))
    }

    async fn seeded_store() -> (BlockStore, SchemaRef) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut bs = BlockStore::new(object_store, base);
        let schema = log_schema();

        let mut api = Labels::new();
        api.insert("app", "api");
        let fp = api.fingerprint();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![fp, fp])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["hello", "world"])),
            ],
        )
        .unwrap();

        let meta = bs
            .writer()
            .write_block("t", "blocks/b1.parquet", schema.clone(), &[batch])
            .await
            .unwrap();
        bs.index_mut().add_series("t", fp, &api);
        bs.index_mut().add_block(&meta);
        (bs, schema)
    }

    #[tokio::test]
    async fn from_config_inmemory_builds_usable_store() {
        use krabka_object_store::ObjectStoreConfig;

        let base = url::Url::parse("memory:///").unwrap();
        let bs = BlockStore::from_config(&ObjectStoreConfig::InMemory, base).unwrap();
        let store = bs.object_store();
        let path = object_store::path::Path::from("t/x");
        store
            .put(&path, object_store::PutPayload::from(b"hi".to_vec()))
            .await
            .unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert2::assert!(&got[..] == b"hi");
    }

    #[tokio::test]
    async fn scan_returns_rows_for_matching_series() {
        let (bs, schema) = seeded_store().await;
        let matchers = [LabelMatcher::new("app", MatchOp::Eq, "api")];

        let (ctx, table) = bs
            .scan_context("t", &matchers, 0, 1_000, schema)
            .await
            .unwrap();

        let df = ctx
            .sql(&format!("SELECT line FROM {table} ORDER BY timestamp"))
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let first = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(0))
            .or_else(|| {
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .map(|a| a.value(0))
            })
            .or_else(|| {
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .map(|a| a.value(0))
            })
            .expect("line column is utf8");
        assert2::assert!(total == 2);
        assert2::assert!(first == "hello");
    }

    #[tokio::test]
    async fn index_returns_the_stores_own_populated_index() {
        // The accessor must hand back the store's real index, not a fresh
        // default one: the seeded `app=api` series must resolve.
        let (bs, _schema) = seeded_store().await;
        let mut api = Labels::new();
        api.insert("app", "api");
        let got = bs
            .index()
            .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert2::assert!(got == std::collections::BTreeSet::from([api.fingerprint()]));
    }

    #[tokio::test]
    async fn cloned_blockstore_shares_index_until_mutated() {
        let (bs, _schema) = seeded_store().await;
        let cloned = bs.clone();
        assert2::assert!(Arc::ptr_eq(&bs.index, &cloned.index));

        let mut mutated = cloned.clone();
        let mut web = Labels::new();
        web.insert("app", "web");
        mutated.index_mut().add_series("t", web.fingerprint(), &web);

        assert2::assert!(!Arc::ptr_eq(&bs.index, &mutated.index));
        assert2::assert!(
            bs.index()
                .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "web")])
                .unwrap()
                == std::collections::BTreeSet::new()
        );
        assert2::assert!(
            mutated
                .index()
                .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "web")])
                .unwrap()
                == std::collections::BTreeSet::from([web.fingerprint()])
        );
    }

    #[tokio::test]
    async fn scan_block_keys_reads_named_blocks() {
        let (bs, schema) = seeded_store().await;
        let (ctx, table) = bs
            .scan_block_keys(&["blocks/b1.parquet".to_string()], schema)
            .await
            .unwrap();
        // Table name is the fixed logical name, not a stub string.
        assert2::assert!(table == "logs");
        let df = ctx.sql(&format!("SELECT line FROM {table}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert2::assert!(total == 2);
    }

    #[tokio::test]
    async fn scan_block_row_groups_reads_selected_groups() {
        let (bs, schema) = seeded_store().await;
        let (ctx, table) = bs
            .scan_block_row_groups("blocks/b1.parquet", &[0], schema)
            .await
            .unwrap();
        assert2::assert!(table == "logs");
        let df = ctx.sql(&format!("SELECT line FROM {table}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert2::assert!(total == 2);
    }

    #[tokio::test]
    async fn block_read_max_reaches_metadata_row_groups_and_empty_like() {
        let (bs, schema) = seeded_store().await;
        let capped = BlockStore::new_with_block_read_max(
            bs.object_store(),
            url::Url::parse("memory:///").unwrap(),
            krabka_units::bytes(1),
        );

        assert2::assert!(
            capped
                .read_row_group_metadata("blocks/b1.parquet")
                .await
                .is_err()
        );
        assert2::assert!(
            capped
                .scan_block_row_groups("blocks/b1.parquet", &[0], schema)
                .await
                .is_err()
        );
        assert2::assert!(
            capped
                .empty_like()
                .read_row_group_metadata("blocks/b1.parquet")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn scan_with_no_matching_blocks_returns_empty_shape() {
        let (bs, schema) = seeded_store().await;
        let matchers = [LabelMatcher::new("app", MatchOp::Eq, "absent")];

        let (ctx, table) = bs
            .scan_context("t", &matchers, 0, 1_000, schema)
            .await
            .unwrap();
        let df = ctx.sql(&format!("SELECT line FROM {table}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert2::assert!(total == 0);
    }
}

mod block_store;
mod scan_table_request;
mod table_name;

pub use block_store::BlockStore;
pub use scan_table_request::ScanTableRequest;
use table_name::TABLE_NAME;
