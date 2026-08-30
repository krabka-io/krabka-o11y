use super::{NativeSymbol, loader_frames};

pub(crate) fn loader_frames_from_bytes(bytes: &[u8], address: u64) -> Option<Vec<NativeSymbol>> {
    // `addr2line::Loader` requires a filesystem path. Use a `NamedTempFile`
    // (O_EXCL, 0600, auto-removed on drop) instead of a predictable temp path
    // so the untrusted blob cannot be targeted by a symlink/TOCTOU attack.
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new().ok()?;
    file.write_all(bytes).ok()?;
    file.flush().ok()?;
    loader_frames(file.path(), address)
}
