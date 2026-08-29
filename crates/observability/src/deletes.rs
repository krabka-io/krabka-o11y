#[allow(clippy::wildcard_imports)]
use super::*;

#[derive(Clone, Default)]
pub(crate) struct CompactorDeleteState {
    pub(crate) delete_requests: SharedLogDeleteRequests,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub(crate) struct CompactorDeleteRequests {
    pub(crate) next_id: u64,
    pub(crate) requests: Vec<CompactorDeleteRequest>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct CompactorDeleteRequest {
    pub(crate) tenant: String,
    pub(crate) request_id: String,
    pub(crate) query: String,
    pub(crate) start_time: i64,
    pub(crate) end_time: i64,
    pub(crate) status: String,
    pub(crate) created_at: i64,
}

#[derive(Clone)]
pub(crate) struct ActiveLogDeleteFilter {
    pub(crate) time_range: TimeRange,
    pub(crate) query: StreamQuery,
}

#[derive(Serialize)]
pub(crate) struct CompactorDeleteRequestResponse {
    pub(crate) request_id: String,
    pub(crate) start_time: i64,
    pub(crate) end_time: i64,
    pub(crate) query: String,
    pub(crate) status: String,
    pub(crate) created_at: i64,
}

pub(crate) struct CreateDeleteRequestParams {
    pub(crate) query: String,
    pub(crate) start_time: i64,
    pub(crate) end_time: i64,
}

pub(crate) struct ListDeleteRequestsParams {
    pub(crate) start_time: Option<i64>,
    pub(crate) end_time: Option<i64>,
}

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

pub(crate) fn log_delete_requests_path(root: &FsPath) -> PathBuf {
    root.join("log-delete-requests.json")
}

pub(crate) fn read_log_delete_requests(
    path: &FsPath,
) -> Result<CompactorDeleteRequests, LogDeleteRequestStoreError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == ErrorKind::NotFound => {
            return Ok(CompactorDeleteRequests::default());
        }
        Err(source) => {
            return Err(LogDeleteRequestStoreError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    serde_json::from_slice(&bytes).map_err(|source| LogDeleteRequestStoreError::Json {
        path: path.to_path_buf(),
        source,
    })
}

pub(crate) fn write_log_delete_requests(
    path: &FsPath,
    requests: &CompactorDeleteRequests,
) -> Result<(), LogDeleteRequestStoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| LogDeleteRequestStoreError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let tmp_path = path.with_file_name(".log-delete-requests.json.tmp");
    let payload =
        serde_json::to_vec_pretty(requests).map_err(|source| LogDeleteRequestStoreError::Json {
            path: path.to_path_buf(),
            source,
        })?;
    std::fs::write(&tmp_path, payload).map_err(|source| LogDeleteRequestStoreError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    std::fs::rename(&tmp_path, path).map_err(|source| LogDeleteRequestStoreError::Io {
        path: path.to_path_buf(),
        source,
    })
}
