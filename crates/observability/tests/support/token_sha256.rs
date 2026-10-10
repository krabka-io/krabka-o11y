//! The token digest that a credentials file names, shared by the
//! `server_security` unit tests and the suites that load a credentials file.
//!
//! Each reaches this file with `#[path]`, so it names only external crates.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

/// The lowercase hex SHA-256 digest of `token`, as a token digest flag takes
/// it.
pub fn sha256_hex(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .fold(String::new(), |mut hex, byte| {
            write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
            hex
        })
}
