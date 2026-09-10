use super::{BlockStoreError, Result};

/// A cursor over an encoded index shard.
///
/// Every read is bounds-checked and reports a [`BlockStoreError::InvalidBlock`]
/// rather than panicking: a shard comes from shared object storage and, per the
/// threat model, may be truncated or corrupt. A decoder that indexes a slice
/// directly would abort the process on such an object.
pub(crate) struct ByteReader<'bytes> {
    bytes: &'bytes [u8],
    at: usize,
}

impl<'bytes> ByteReader<'bytes> {
    pub(crate) const fn new(bytes: &'bytes [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn short(what: &str) -> BlockStoreError {
        BlockStoreError::InvalidBlock(format!("index shard ends inside {what}"))
    }

    pub(crate) fn take(&mut self, len: usize, what: &str) -> Result<&'bytes [u8]> {
        let end = self.at.checked_add(len).ok_or_else(|| Self::short(what))?;
        let slice = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| Self::short(what))?;
        self.at = end;
        Ok(slice)
    }

    pub(crate) fn u8(&mut self, what: &str) -> Result<u8> {
        Ok(self.take(1, what)?[0])
    }

    pub(crate) fn u64_le(&mut self, what: &str) -> Result<u64> {
        let bytes = self.take(8, what)?;
        let mut fixed = [0_u8; 8];
        fixed.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(fixed))
    }

    pub(crate) fn uvarint(&mut self, what: &str) -> Result<u64> {
        let mut value = 0_u64;
        let mut shift = 0_u32;
        loop {
            let byte = self.u8(what)?;
            if shift >= 64 || (shift == 63 && byte > 1) {
                return Err(BlockStoreError::InvalidBlock(format!(
                    "index shard varint in {what} overflows a u64"
                )));
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
    }

    pub(crate) fn ivarint(&mut self, what: &str) -> Result<i64> {
        let raw = self.uvarint(what)?;
        let magnitude = i64::from_le_bytes((raw >> 1).to_le_bytes());
        let sign = i64::from_le_bytes((raw & 1).to_le_bytes());
        Ok(magnitude ^ -sign)
    }

    /// A varint length followed by that many bytes, decoded as UTF-8.
    pub(crate) fn string(&mut self, what: &str) -> Result<String> {
        let len = usize::try_from(self.uvarint(what)?).map_err(|_| {
            BlockStoreError::InvalidBlock(format!("index shard {what} is too long"))
        })?;
        let bytes = self.take(len, what)?;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            BlockStoreError::InvalidBlock(format!("index shard {what} is not UTF-8: {error}"))
        })
    }

    /// A varint count, rejected when it cannot address that many elements.
    pub(crate) fn count(&mut self, what: &str) -> Result<usize> {
        let count = self.uvarint(what)?;
        let count = usize::try_from(count).map_err(|_| {
            BlockStoreError::InvalidBlock(format!("index shard {what} count does not fit a usize"))
        })?;
        // A count is an upper bound on how many elements follow, and each
        // element is at least one byte. Rejecting a count larger than the bytes
        // that remain stops a corrupt header from reserving gigabytes before
        // the read that would have failed anyway.
        if count > self.bytes.len().saturating_sub(self.at) {
            return Err(BlockStoreError::InvalidBlock(format!(
                "index shard declares {count} {what} but has {} bytes left",
                self.bytes.len().saturating_sub(self.at)
            )));
        }
        Ok(count)
    }

    /// A varint read as a count that is a value rather than an element count,
    /// so nothing bounds it but the width of a `usize`.
    pub(crate) fn count_unbounded(&mut self, what: &str) -> Result<usize> {
        let value = self.uvarint(what)?;
        usize::try_from(value).map_err(|_| {
            BlockStoreError::InvalidBlock(format!("index shard {what} does not fit a usize"))
        })
    }

    pub(crate) const fn is_exhausted(&self) -> bool {
        self.at >= self.bytes.len()
    }
}
