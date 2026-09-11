use super::{Path, WalClientSecurityError, fs};

/// Reads a SASL password from `path`, without its trailing line breaks.
///
/// An editor or `echo` ends the file with a line break, and the broker would
/// refuse a password that kept it. Line breaks are the only characters the
/// function removes, because spaces can be part of a password.
pub(crate) fn read_password_file(path: &Path) -> Result<String, WalClientSecurityError> {
    let bytes = fs::read(path).map_err(|source| WalClientSecurityError::UnreadableFile {
        flag: "--wal-sasl-password-path",
        path: path.to_path_buf(),
        source,
    })?;
    // `FromUtf8Error` holds the bytes, so it must not reach the error.
    let contents =
        String::from_utf8(bytes).map_err(|_| WalClientSecurityError::PasswordFileNotUtf8 {
            path: path.to_path_buf(),
        })?;
    let password = contents.trim_end_matches(['\r', '\n']);
    if password.is_empty() {
        return Err(WalClientSecurityError::EmptyPasswordFile {
            path: path.to_path_buf(),
        });
    }
    Ok(password.to_owned())
}
