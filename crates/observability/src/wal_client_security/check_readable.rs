use super::{Path, WalClientSecurityError, fs};

/// Returns an error that names `flag` and `path` when the process cannot read
/// the file.
///
/// The check reads the whole file, because opening a directory succeeds on
/// Linux and only the read fails. It drops the bytes at once and puts none of
/// them in the error.
pub(crate) fn check_readable(
    flag: &'static str,
    path: &Path,
) -> Result<(), WalClientSecurityError> {
    fs::read(path)
        .map(drop)
        .map_err(|source| WalClientSecurityError::UnreadableFile {
            flag,
            path: path.to_path_buf(),
            source,
        })
}
