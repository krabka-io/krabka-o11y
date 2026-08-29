#[derive(Clone, Default)]
struct CompactorDeleteState {
    delete_requests: SharedLogDeleteRequests,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct CompactorDeleteRequests {
    next_id: u64,
    requests: Vec<CompactorDeleteRequest>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CompactorDeleteRequest {
    tenant: String,
    request_id: String,
    query: String,
    start_time: i64,
    end_time: i64,
    status: String,
    created_at: i64,
}

#[derive(Clone)]
struct ActiveLogDeleteFilter {
    time_range: TimeRange,
    query: StreamQuery,
}

#[derive(Serialize)]
struct CompactorDeleteRequestResponse {
    request_id: String,
    start_time: i64,
    end_time: i64,
    query: String,
    status: String,
    created_at: i64,
}

struct CreateDeleteRequestParams {
    query: String,
    start_time: i64,
    end_time: i64,
}

struct ListDeleteRequestsParams {
    start_time: Option<i64>,
    end_time: Option<i64>,
}

impl SharedLogDeleteRequests {
    fn from_data_root(root: impl AsRef<FsPath>) -> Result<Self, LogDeleteRequestStoreError> {
        let path = log_delete_requests_path(root.as_ref());
        Ok(Self {
            inner: Arc::new(Mutex::new(read_log_delete_requests(&path)?)),
            storage_path: Some(Arc::new(path)),
        })
    }

    fn persist(&self) -> Result<(), LogDeleteRequestStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let requests = self.inner.lock().expect("compactor delete state poisoned");
        write_log_delete_requests(path, &requests)
    }

    fn refresh(&self) -> Result<(), LogDeleteRequestStoreError> {
        let Some(path) = &self.storage_path else {
            return Ok(());
        };
        let requests = read_log_delete_requests(path)?;
        *self.inner.lock().expect("compactor delete state poisoned") = requests;
        Ok(())
    }
}

fn log_delete_requests_path(root: &FsPath) -> PathBuf {
    root.join("log-delete-requests.json")
}

fn read_log_delete_requests(
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

fn write_log_delete_requests(
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

