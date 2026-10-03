use super::TsdbImportError;

/// A cursor over one section of a TSDB file.
///
/// Every read checks the remaining length first and returns
/// [`TsdbImportError::Truncated`] for the section rather than panicking.
pub struct ByteReader<'a> {
    data: &'a [u8],
    position: usize,
    section: &'static str,
}

impl<'a> ByteReader<'a> {
    pub fn new(data: &'a [u8], section: &'static str) -> Self {
        Self {
            data,
            position: 0,
            section,
        }
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.position
    }

    fn truncated(&self) -> TsdbImportError {
        TsdbImportError::Truncated {
            section: self.section,
        }
    }

    pub fn bytes(&mut self, count: usize) -> Result<&'a [u8], TsdbImportError> {
        let end = self
            .position
            .checked_add(count)
            .filter(|end| *end <= self.data.len())
            .ok_or_else(|| self.truncated())?;
        let bytes = &self.data[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    pub fn u8(&mut self) -> Result<u8, TsdbImportError> {
        Ok(self.bytes(1)?[0])
    }

    pub fn be32(&mut self) -> Result<u32, TsdbImportError> {
        let bytes = self.bytes(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn be64(&mut self) -> Result<u64, TsdbImportError> {
        let (high, low) = (u64::from(self.be32()?), u64::from(self.be32()?));
        Ok((high << 32) | low)
    }

    pub fn uvarint(&mut self) -> Result<u64, TsdbImportError> {
        let mut value = 0_u64;
        for index in 0..10 {
            let byte = self.u8()?;
            if index == 9 && byte > 1 {
                return Err(TsdbImportError::InvalidIndex(format!(
                    "{} holds a varint that overflows 64 bits",
                    self.section
                )));
            }
            value |= u64::from(byte & 0x7f) << (7 * index);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(self.truncated())
    }

    pub fn varint(&mut self) -> Result<i64, TsdbImportError> {
        let raw = self.uvarint()?;
        let magnitude = (raw >> 1).cast_signed();
        Ok(if raw & 1 == 0 { magnitude } else { !magnitude })
    }

    /// Reads a uvarint length and checks that the section still holds it.
    pub fn uvarint_len(&mut self) -> Result<usize, TsdbImportError> {
        let length = self.uvarint()?;
        usize::try_from(length)
            .ok()
            .filter(|length| *length <= self.remaining())
            .ok_or_else(|| self.truncated())
    }

    pub fn uvarint_bytes(&mut self) -> Result<&'a [u8], TsdbImportError> {
        let length = self.uvarint_len()?;
        self.bytes(length)
    }
}
