use super::Write;

/// The destination of the repair audit log.
///
/// A repair calls [`sync`](Self::sync) after each line, and it starts a
/// delete only after the intent line of that delete is synced.
pub trait RepairLogWriter: Write {
    /// Makes every line written so far durable.
    ///
    /// # Errors
    /// Returns the error of the flush or of the sync.
    fn sync(&mut self) -> std::io::Result<()>;
}

impl RepairLogWriter for std::fs::File {
    fn sync(&mut self) -> std::io::Result<()> {
        self.flush()?;
        self.sync_data()
    }
}

/// An in-memory log, for a caller that keeps the lines itself.
impl RepairLogWriter for Vec<u8> {
    fn sync(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
