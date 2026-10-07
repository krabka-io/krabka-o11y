use super::{
    Arc, CompactorDeleteRequests, FsPath, LogDeleteRequestStoreError, Mutex,
    SharedLogDeleteRequests, log_delete_requests_path, read_log_delete_requests,
    write_log_delete_requests,
};

impl SharedLogDeleteRequests {
    pub(crate) fn from_data_root(
        root: impl AsRef<FsPath>,
    ) -> Result<Self, LogDeleteRequestStoreError> {
        let path = log_delete_requests_path(root.as_ref());
        Ok(Self {
            inner: Arc::new(Mutex::new(read_log_delete_requests(&path)?)),
            storage_path: Some(Arc::new(path)),
        })
    }

    pub(crate) fn persist(
        &self,
        requests: &CompactorDeleteRequests,
    ) -> Result<(), LogDeleteRequestStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        write_log_delete_requests(path, requests)
    }

    pub(crate) fn refresh(&self) -> Result<(), LogDeleteRequestStoreError> {
        self.refresh_from(read_log_delete_requests)
    }

    fn refresh_from(
        &self,
        read: impl FnOnce(&FsPath) -> Result<CompactorDeleteRequests, LogDeleteRequestStoreError>,
    ) -> Result<(), LogDeleteRequestStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let mut requests = self.inner.lock().expect("compactor delete state poisoned");
        *requests = read(path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
