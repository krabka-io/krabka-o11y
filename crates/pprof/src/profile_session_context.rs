use std::sync::{Arc, OnceLock};

use datafusion::{
    catalog::{CatalogProviderList, MemoryCatalogProviderList},
    execution::SessionStateDefaults,
    prelude::SessionContext,
};

static TEMPLATE: OnceLock<SessionContext> = OnceLock::new();

/// Create a profile query context with shared functions and a private catalog.
///
/// Function and runtime initialization is paid once. Every returned context
/// has its own catalog, so concurrent queries can safely register `samples`.
#[must_use]
pub fn profile_session_context() -> SessionContext {
    let mut state = TEMPLATE.get_or_init(SessionContext::new).state();
    let catalogs = Arc::new(MemoryCatalogProviderList::new());
    let catalog = SessionStateDefaults::default_catalog(
        state.config(),
        state.table_factories(),
        state.runtime_env(),
    );
    catalogs.register_catalog(
        state.config_options().catalog.default_catalog.clone(),
        Arc::new(catalog),
    );
    state.register_catalog_list(catalogs);
    SessionContext::new_with_state(state)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::UInt64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::catalog::MemTable;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_queries_keep_private_tables_and_the_default_functions() {
        let tasks = (0..8_u64).map(|id| {
            tokio::spawn(async move {
                let ctx = profile_session_context();
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "value",
                        DataType::UInt64,
                        false,
                    )])),
                    vec![Arc::new(UInt64Array::from(vec![id]))],
                )
                .unwrap();
                ctx.register_table(
                    "samples",
                    Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap()),
                )
                .unwrap();
                tokio::task::yield_now().await;
                let actual = ctx
                    .sql("SELECT value FROM samples WHERE lower('API') = 'api'")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                assert2::assert!(actual == vec![batch]);
            })
        });
        for task in tasks.collect::<Vec<_>>() {
            task.await.unwrap();
        }
    }
}
