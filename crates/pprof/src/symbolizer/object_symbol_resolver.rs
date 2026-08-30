use super::{
    Arc, NativeResolver, NativeSymbol, PathBuf, SymbolizeRequest, loader_frames,
    loader_frames_from_bytes, nearest_symbol_name, parse_object_guarded,
};

#[derive(Clone, Debug)]
pub struct ObjectSymbolResolver {
    pub(crate) bytes: Arc<Vec<u8>>,
    pub(crate) path: Option<PathBuf>,
}

impl ObjectSymbolResolver {
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, String> {
        parse_object_guarded(bytes.as_slice())?;
        Ok(Self {
            bytes: Arc::new(bytes),
            path: None,
        })
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        let bytes = std::fs::read(&path).map_err(|err| err.to_string())?;
        parse_object_guarded(bytes.as_slice())?;
        Ok(Self {
            bytes: Arc::new(bytes),
            path: Some(path),
        })
    }
}

impl NativeResolver for ObjectSymbolResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        // The bytes may be an untrusted, crafted ELF/DWARF blob. Contain any
        // parser panic so a single malicious artifact cannot crash the worker.
        let bytes = Arc::clone(&self.bytes);
        let path = self.path.clone();
        let filename = request.filename.clone();
        let address = request.address;
        std::panic::catch_unwind(move || {
            let object = object::File::parse(bytes.as_slice()).ok()?;
            let frames = path
                .as_ref()
                .and_then(|path| loader_frames(path, address))
                .or_else(|| loader_frames_from_bytes(&bytes, address));
            if let Some(frames) = frames
                && !frames.is_empty()
            {
                return Some(frames);
            }
            let function = nearest_symbol_name(&object, address)
                .unwrap_or_else(|| format!("{filename}+0x{address:x}"));
            Some(vec![NativeSymbol {
                function,
                file: filename,
                line: 0,
            }])
        })
        .unwrap_or(None)
    }
}
