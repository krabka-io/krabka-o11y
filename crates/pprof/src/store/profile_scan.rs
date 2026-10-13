use arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;

use super::{Arc, ProfileError, SessionContext, SymbolSource};
use crate::profile_session_context;

/// A selected samples table plus the symbol source that resolves its raw ids.
pub struct ProfileScan {
    pub ctx: SessionContext,
    pub samples_table: String,
    pub symbols: Arc<dyn SymbolSource>,
}

impl ProfileScan {
    /// Registers a profile sample table and pairs it with its symbol source.
    pub fn from_mem_table(
        table: MemTable,
        symbols: Arc<dyn SymbolSource>,
    ) -> Result<Self, ProfileError> {
        let ctx = profile_session_context();
        let samples_table = "samples".to_string();
        ctx.register_table(&samples_table, Arc::new(table))
            .map_err(|err| ProfileError::Store(err.to_string()))?;
        Ok(Self {
            ctx,
            samples_table,
            symbols,
        })
    }

    /// Plans `sql` against the scan's session and collects every batch it
    /// answers with.
    ///
    /// # Errors
    /// Returns [`ProfileError::Plan`] when `sql` does not plan, and
    /// [`ProfileError::Exec`] when the plan fails to run.
    pub async fn collect_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, ProfileError> {
        self.ctx
            .sql(sql)
            .await
            .map_err(|err| ProfileError::Plan(err.to_string()))?
            .collect()
            .await
            .map_err(|err| ProfileError::Exec(err.to_string()))
    }
}
