use super::{BTreeMap, ByteReader, TsdbImportError};

/// The deleted time ranges of a block, keyed by index series reference.
///
/// Each interval is inclusive at both ends, as Prometheus stores it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Tombstones {
    intervals: BTreeMap<u64, Vec<(i64, i64)>>,
}

const MAGIC: u32 = 0x0130_BA30;
const FORMAT_V1: u8 = 1;

impl Tombstones {
    /// Parses a `tombstones` file: a big-endian magic, a format byte, the
    /// `(series, mint, maxt)` records, and a big-endian CRC32C over the
    /// records.
    pub fn parse(file: &[u8], max_records: u64) -> Result<Self, TsdbImportError> {
        const SECTION: &str = "tombstones";
        if file.len() < 9 {
            return Err(TsdbImportError::Truncated { section: SECTION });
        }
        let (body, crc) = file.split_at(file.len() - 4);
        let mut reader = ByteReader::new(body, SECTION);
        let magic = reader.be32()?;
        if magic != MAGIC {
            return Err(TsdbImportError::BadMagic {
                file: SECTION,
                found: magic,
                expected: MAGIC,
            });
        }
        let version = reader.u8()?;
        if version != FORMAT_V1 {
            return Err(TsdbImportError::UnsupportedTombstonesVersion(version));
        }
        let expected = u32::from_be_bytes([crc[0], crc[1], crc[2], crc[3]]);
        if crc32c::crc32c(&body[5..]) != expected {
            return Err(TsdbImportError::Checksum {
                section: SECTION.to_owned(),
            });
        }
        let mut intervals = BTreeMap::<u64, Vec<(i64, i64)>>::new();
        let mut records = 0_u64;
        while reader.remaining() > 0 {
            records += 1;
            if records > max_records {
                return Err(TsdbImportError::LimitExceeded {
                    limit: "tombstone records",
                    value: records,
                    max: max_records,
                });
            }
            let series = reader.uvarint()?;
            let min_time = reader.varint()?;
            let max_time = reader.varint()?;
            intervals
                .entry(series)
                .or_default()
                .push((min_time, max_time));
        }
        Ok(Self { intervals })
    }

    /// Whether a sample of `series` at `timestamp` was deleted.
    pub fn deletes(&self, series: u64, timestamp: i64) -> bool {
        self.intervals.get(&series).is_some_and(|intervals| {
            intervals
                .iter()
                .any(|(min_time, max_time)| (*min_time..=*max_time).contains(&timestamp))
        })
    }

    /// The series references that hold at least one deleted interval.
    pub fn series(&self) -> impl Iterator<Item = u64> + '_ {
        self.intervals.keys().copied()
    }
}
