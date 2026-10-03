use super::{ByteReader, ChunkEncoding, TsdbImportError};

/// The chunk segment files of a block, in file-name order.
///
/// A chunk reference holds the segment's position in that order in its high
/// 32 bits and the byte offset within the segment in its low 32 bits.
pub struct ChunkSegments<'a> {
    segments: Vec<&'a [u8]>,
}

const MAGIC: u32 = 0x85BD_40DD;
const FORMAT_V1: u8 = 1;
const HEADER_LEN: usize = 8;

impl<'a> ChunkSegments<'a> {
    /// Checks each segment header. `segments` must already be in file-name
    /// order.
    pub fn new(segments: Vec<&'a [u8]>) -> Result<Self, TsdbImportError> {
        for (index, segment) in segments.iter().enumerate() {
            let mut reader = ByteReader::new(segment, "chunk segment header");
            let magic = reader.be32()?;
            if magic != MAGIC {
                return Err(TsdbImportError::BadMagic {
                    file: "chunk segment",
                    found: magic,
                    expected: MAGIC,
                });
            }
            let version = reader.u8()?;
            if version != FORMAT_V1 {
                return Err(TsdbImportError::UnsupportedChunkFormat {
                    segment: index,
                    version,
                });
            }
        }
        Ok(Self { segments })
    }

    /// Returns the verified encoding and data of the chunk at `reference`.
    pub fn chunk(&self, reference: u64) -> Result<(ChunkEncoding, &'a [u8]), TsdbImportError> {
        let out_of_range = || TsdbImportError::ChunkReference { reference };
        let segment = usize::try_from(reference >> 32).map_err(|_| out_of_range())?;
        let offset = usize::try_from(reference & 0xffff_ffff).map_err(|_| out_of_range())?;
        let file = self.segments.get(segment).ok_or_else(out_of_range)?;
        if offset < HEADER_LEN || offset >= file.len() {
            return Err(out_of_range());
        }
        let mut reader = ByteReader::new(&file[offset..], "chunk");
        let length = reader.uvarint()?;
        let length = usize::try_from(length)
            .ok()
            .filter(|length| {
                length
                    .checked_add(5)
                    .is_some_and(|framed| framed <= reader.remaining())
            })
            .ok_or(TsdbImportError::Truncated { section: "chunk" })?;
        let covered = reader.bytes(length + 1)?;
        let expected = reader.be32()?;
        if crc32c::crc32c(covered) != expected {
            return Err(TsdbImportError::Checksum {
                section: format!("chunk at reference {reference:#x}"),
            });
        }
        let encoding =
            ChunkEncoding::from_byte(covered[0]).ok_or(TsdbImportError::UnknownChunkEncoding {
                reference,
                encoding: covered[0],
            })?;
        Ok((encoding, &covered[1..]))
    }
}
