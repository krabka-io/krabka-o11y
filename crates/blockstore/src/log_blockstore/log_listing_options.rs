use super::{Arc, ListingOptions, ParquetFormat, ParquetOptions, TableParquetOptions};

/// The `ListingOptions` every planned log scan is built from, whichever source
/// holds the blocks.
///
/// Both the local-filesystem source and the object-store source read the same
/// Parquet layout, written by the same encoder, so they read it through the
/// same options. That is what makes the two sources one scan rather than two.
///
/// `pushdown_filters` is the setting worth naming. With it off a predicate on
/// `timestamp_ns`, `series_fingerprint` or `line` only prunes row groups by
/// their statistics, and every row that survives the prune is decoded and
/// handed up to a `FilterExec` above the scan. With it on the same predicate
/// also becomes a Parquet `RowFilter`, so the rows it rejects are never
/// decoded into Arrow at all. A log block written by `encode_log_block` is a
/// single row group at anything under a million rows, so statistics pruning
/// has nothing to prune inside one block, and the row filter is the whole of
/// the saving. Correctness does not rest on it either way: the provider
/// advertises [`TableProviderFilterPushDown::Inexact`], so the `FilterExec`
/// stays in the plan unless `DataFusion` itself decides the scan has taken the
/// predicate over exactly.
///
/// [`TableProviderFilterPushDown::Inexact`]: datafusion::datasource::provider::TableProviderFilterPushDown::Inexact
pub(crate) fn log_listing_options() -> ListingOptions {
    let options = TableParquetOptions {
        global: ParquetOptions {
            pushdown_filters: true,
            ..ParquetOptions::default()
        },
        ..TableParquetOptions::default()
    };
    ListingOptions::new(Arc::new(ParquetFormat::default().with_options(options)))
        .with_file_extension(".parquet")
}
