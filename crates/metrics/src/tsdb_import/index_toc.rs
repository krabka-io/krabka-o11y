use super::{ByteReader, TsdbImportError};

/// The table of contents at the end of a TSDB index file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexToc {
    pub symbols: u64,
    pub series: u64,
    pub label_indices: u64,
    pub label_indices_table: u64,
    pub postings: u64,
    pub postings_table: u64,
}

/// Six big-endian `u64` offsets and a CRC32C over them.
pub const INDEX_TOC_LEN: usize = 52;
const HEADER_LEN: u64 = 5;

impl IndexToc {
    /// Reads and checks the TOC. The sections must appear in the order that
    /// the Prometheus writer uses, and all of them must start after the file
    /// header and before the TOC.
    pub fn read(index: &[u8]) -> Result<Self, TsdbImportError> {
        let start = index
            .len()
            .checked_sub(INDEX_TOC_LEN)
            .ok_or(TsdbImportError::Truncated {
                section: "index TOC",
            })?;
        let toc = &index[start..];
        let expected = u32::from_be_bytes([toc[48], toc[49], toc[50], toc[51]]);
        if crc32c::crc32c(&toc[..48]) != expected {
            return Err(TsdbImportError::Checksum {
                section: "index TOC".to_owned(),
            });
        }
        let mut reader = ByteReader::new(&toc[..48], "index TOC");
        let parsed = Self {
            symbols: reader.be64()?,
            series: reader.be64()?,
            label_indices: reader.be64()?,
            label_indices_table: reader.be64()?,
            postings: reader.be64()?,
            postings_table: reader.be64()?,
        };
        let end = u64::try_from(start).unwrap_or(u64::MAX);
        let ordered = [
            HEADER_LEN,
            parsed.symbols,
            parsed.series,
            parsed.label_indices,
            parsed.postings,
            parsed.label_indices_table,
            parsed.postings_table,
            end,
        ];
        if ordered.windows(2).any(|pair| pair[0] > pair[1]) || parsed.postings_table == end {
            return Err(TsdbImportError::InvalidIndex(format!(
                "table of contents sections are out of order: {parsed:?}"
            )));
        }
        Ok(parsed)
    }
}
