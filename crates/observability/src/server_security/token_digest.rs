use std::fmt;

use sha2::{Digest, Sha256};
use subtle::{Choice, ConstantTimeEq};

/// The SHA-256 digest of a bearer token.
///
/// It has no `PartialEq`, so every comparison goes through the constant-time
/// [`ct_eq`](Self::ct_eq).
#[derive(Clone, Copy)]
pub struct TokenDigest([u8; 32]);

impl TokenDigest {
    /// Parses a digest written as exactly 64 lowercase hexadecimal characters.
    pub fn parse(hex: &str) -> Option<Self> {
        let hex = hex.as_bytes();
        if hex.len() != 64 {
            return None;
        }
        let mut digest = [0_u8; 32];
        for (byte, pair) in digest.iter_mut().zip(hex.chunks_exact(2)) {
            *byte =
                (Self::lowercase_hex_value(pair[0])? << 4) | Self::lowercase_hex_value(pair[1])?;
        }
        Some(Self(digest))
    }

    fn lowercase_hex_value(character: u8) -> Option<u8> {
        match character {
            b'0'..=b'9' => Some(character - b'0'),
            b'a'..=b'f' => Some(character - b'a' + 10),
            _ => None,
        }
    }

    /// The digest of a token that a request presented.
    pub fn of_token(token: &[u8]) -> Self {
        Self(Sha256::digest(token).into())
    }

    /// Compares two digests in constant time.
    pub fn ct_eq(&self, other: &Self) -> Choice {
        self.0.ct_eq(&other.0)
    }
}

impl fmt::Debug for TokenDigest {
    // A digest of a short token is a way to search for the token, so it stays
    // out of logs and panic messages.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TokenDigest(..)")
    }
}
