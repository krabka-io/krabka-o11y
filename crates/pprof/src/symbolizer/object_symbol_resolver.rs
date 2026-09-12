use super::{
    Arc, Mutex, NativeResolver, NativeSymbol, PathBuf, SymbolizeRequest, loader_frames,
    lock_recover,
};

#[derive(Clone)]
pub struct ObjectSymbolResolver {
    pub(crate) loader: Arc<Mutex<addr2line::Loader>>,
    pub(crate) artifact_size: usize,
}

impl ObjectSymbolResolver {
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, String> {
        use std::io::Write as _;
        let mut file = tempfile::NamedTempFile::new().map_err(|err| err.to_string())?;
        file.write_all(&bytes).map_err(|err| err.to_string())?;
        file.flush().map_err(|err| err.to_string())?;
        let loader = addr2line::Loader::new(file.path()).map_err(|err| err.to_string())?;
        Ok(Self {
            loader: Arc::new(Mutex::new(loader)),
            artifact_size: bytes.len(),
        })
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        let artifact_size = usize::try_from(
            std::fs::metadata(&path)
                .map_err(|err| err.to_string())?
                .len(),
        )
        .unwrap_or(usize::MAX);
        let loader = addr2line::Loader::new(&path).map_err(|err| err.to_string())?;
        Ok(Self {
            loader: Arc::new(Mutex::new(loader)),
            artifact_size,
        })
    }

    #[must_use]
    pub(crate) const fn artifact_size(&self) -> usize {
        self.artifact_size
    }
}

impl NativeResolver for ObjectSymbolResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        // The bytes may be an untrusted, crafted ELF/DWARF blob. Contain any
        // parser panic so a single malicious artifact cannot crash the worker.
        let loader = Arc::clone(&self.loader);
        let filename = request.filename.clone();
        let address = request.address;
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let loader = lock_recover(&loader);
            let frames = loader_frames(&loader, address);
            if let Some(frames) = frames
                && !frames.is_empty()
            {
                return Some(frames);
            }
            let function = loader
                .find_symbol(address)
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("{filename}+0x{address:x}"));
            Some(vec![NativeSymbol {
                function,
                file: filename,
                line: 0,
            }])
        }))
        .unwrap_or(None)
    }
}
