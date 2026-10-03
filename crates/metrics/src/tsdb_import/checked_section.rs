use super::{ByteReader, TsdbImportError};

/// Reads a section framed as a big-endian `u32` length, the content, and a
/// big-endian CRC32C of the content, and returns the verified content.
///
/// The symbol table and the postings offset table use this frame.
pub fn checked_section<'a>(
    file: &'a [u8],
    offset: u64,
    section: &'static str,
) -> Result<&'a [u8], TsdbImportError> {
    let start = usize::try_from(offset)
        .ok()
        .filter(|start| *start <= file.len())
        .ok_or(TsdbImportError::Truncated { section })?;
    let mut reader = ByteReader::new(&file[start..], section);
    let length =
        usize::try_from(reader.be32()?).map_err(|_| TsdbImportError::Truncated { section })?;
    let content = reader.bytes(length)?;
    let expected = reader.be32()?;
    if crc32c::crc32c(content) != expected {
        return Err(TsdbImportError::Checksum {
            section: section.to_owned(),
        });
    }
    Ok(content)
}
