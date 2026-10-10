use arrow::record_batch::RecordBatch;

use super::{Arc, ProfileError, SessionContext, SymbolSource};

/// A selected samples table plus the symbol source that resolves its raw ids.
pub struct ProfileScan {
    pub ctx: SessionContext,
    pub samples_table: String,
    pub symbols: Arc<dyn SymbolSource>,
}

impl ProfileScan {
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
