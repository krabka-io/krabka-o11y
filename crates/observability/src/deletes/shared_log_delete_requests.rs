use super::*;

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

    pub(crate) fn persist(&self) -> Result<(), LogDeleteRequestStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let requests = self.inner.lock().expect("compactor delete state poisoned");
        write_log_delete_requests(path, &requests)
    }

    pub(crate) fn refresh(&self) -> Result<(), LogDeleteRequestStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let requests = read_log_delete_requests(path)?;
        *self.inner.lock().expect("compactor delete state poisoned") = requests;
        Ok(())
    }
}
