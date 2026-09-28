use super::{
    ByteReader, ChunkMeta, IndexSeries, IndexToc, TsdbImportError, TsdbImportLimits, format_labels,
};

/// Reads every series entry between the series and label-index offsets of
/// the TOC.
///
/// Each entry starts on a 16-byte boundary, which gives the series its
/// reference. The section offset in the TOC may point before the first
/// boundary. The bytes between entries must be zero. The entry holds its
/// label symbol references and chunk metas, and a CRC32C over both. The
/// checks match the invariants that the Prometheus index writer enforces:
/// series strictly sorted by labels, label names strictly sorted within a
/// series, chunk references non-decreasing, and chunk time ranges ordered and
/// disjoint.
pub fn read_series(
    index: &[u8],
    toc: &IndexToc,
    symbols: &[String],
    limits: &TsdbImportLimits,
) -> Result<Vec<IndexSeries>, TsdbImportError> {
    const SECTION: &str = "index series";
    let start =
        usize::try_from(toc.series).map_err(|_| TsdbImportError::Truncated { section: SECTION })?;
    let end = usize::try_from(toc.label_indices)
        .map_err(|_| TsdbImportError::Truncated { section: SECTION })?;
    let region = index
        .get(..end)
        .ok_or(TsdbImportError::Truncated { section: SECTION })?;
    let mut series = Vec::<IndexSeries>::new();
    let mut position = start;
    let mut last_chunk_reference = 0_u64;
    while position < end {
        let aligned = position.next_multiple_of(16).min(end);
        if region[position..aligned].iter().any(|byte| *byte != 0) {
            return Err(TsdbImportError::InvalidIndex(format!(
                "series padding at {position} is not zero"
            )));
        }
        position = aligned;
        if position == end {
            break;
        }
        let count = u64::try_from(series.len()).unwrap_or(u64::MAX) + 1;
        TsdbImportLimits::check("series", count, limits.max_series)?;
        let mut reader = ByteReader::new(&region[position..], SECTION);
        let content = reader.uvarint_bytes()?;
        let expected = reader.be32()?;
        let reference = u64::try_from(position / 16).unwrap_or(u64::MAX);
        if crc32c::crc32c(content) != expected {
            return Err(TsdbImportError::Checksum {
                section: format!("index series {reference}"),
            });
        }
        let entry = parse_entry(
            content,
            reference,
            symbols,
            limits,
            &mut last_chunk_reference,
        )?;
        if series
            .last()
            .is_some_and(|last| last.labels >= entry.labels)
        {
            return Err(TsdbImportError::SeriesOrder {
                labels: format_labels(&entry.labels),
            });
        }
        series.push(entry);
        position += reader.position();
    }
    Ok(series)
}

fn parse_entry(
    content: &[u8],
    reference: u64,
    symbols: &[String],
    limits: &TsdbImportLimits,
    last_chunk_reference: &mut u64,
) -> Result<IndexSeries, TsdbImportError> {
    let mut reader = ByteReader::new(content, "index series entry");
    let label_count = reader.uvarint()?;
    TsdbImportLimits::check(
        "labels per series",
        label_count,
        limits.max_labels_per_series,
    )?;
    let label_count = usize::try_from(label_count).unwrap_or(usize::MAX);
    let mut labels = Vec::<(String, String)>::with_capacity(label_count);
    for _ in 0..label_count {
        let name = symbol(symbols, reader.uvarint()?)?;
        let value = symbol(symbols, reader.uvarint()?)?;
        if name.is_empty() || value.is_empty() {
            return Err(TsdbImportError::InvalidLabels(format!(
                "series {reference} has an empty label name or value"
            )));
        }
        if labels.last().is_some_and(|(last, _)| last.as_str() >= name) {
            return Err(TsdbImportError::InvalidLabels(format!(
                "series {reference} has label {name:?} out of order or repeated"
            )));
        }
        labels.push((name.to_owned(), value.to_owned()));
    }
    if labels.is_empty() {
        return Err(TsdbImportError::InvalidLabels(format!(
            "series {reference} has no labels"
        )));
    }
    let chunk_count = reader.uvarint()?;
    TsdbImportLimits::check(
        "chunks per series",
        chunk_count,
        limits.max_chunks_per_series,
    )?;
    let chunk_count = usize::try_from(chunk_count).unwrap_or(usize::MAX);
    let mut chunks = Vec::<ChunkMeta>::with_capacity(chunk_count.min(reader.remaining()));
    let invalid = |reason: String| TsdbImportError::InvalidChunk {
        labels: format_labels(&labels),
        reason,
    };
    for index in 0..chunk_count {
        let (min_time, max_time, chunk_reference) = if let Some(previous) = chunks.last() {
            let gap = reader.uvarint()?.cast_signed();
            let min_time = previous
                .max_time
                .checked_add(gap)
                .ok_or_else(|| invalid("chunk time overflows".to_owned()))?;
            let span = reader.uvarint()?.cast_signed();
            let max_time = min_time
                .checked_add(span)
                .ok_or_else(|| invalid("chunk time overflows".to_owned()))?;
            let delta = reader.varint()?;
            let chunk_reference = previous
                .reference
                .cast_signed()
                .checked_add(delta)
                .ok_or_else(|| invalid("chunk reference overflows".to_owned()))?
                .cast_unsigned();
            if min_time <= previous.max_time {
                return Err(invalid(format!(
                    "chunk {index} starts at {min_time}, not after the previous chunk's end {}",
                    previous.max_time
                )));
            }
            (min_time, max_time, chunk_reference)
        } else {
            let min_time = reader.varint()?;
            let span = reader.uvarint()?.cast_signed();
            let max_time = min_time
                .checked_add(span)
                .ok_or_else(|| invalid("chunk time overflows".to_owned()))?;
            (min_time, max_time, reader.uvarint()?)
        };
        if max_time < min_time {
            return Err(invalid(format!("chunk {index} ends before it starts")));
        }
        if chunk_reference < *last_chunk_reference {
            return Err(invalid(format!(
                "chunk reference {chunk_reference:#x} is below the previous reference {:#x}",
                *last_chunk_reference
            )));
        }
        *last_chunk_reference = chunk_reference;
        chunks.push(ChunkMeta {
            min_time,
            max_time,
            reference: chunk_reference,
        });
    }
    if reader.remaining() != 0 {
        return Err(invalid(
            "series entry holds bytes after its chunks".to_owned(),
        ));
    }
    Ok(IndexSeries {
        reference,
        labels,
        chunks,
    })
}

fn symbol(symbols: &[String], reference: u64) -> Result<&str, TsdbImportError> {
    usize::try_from(reference)
        .ok()
        .and_then(|reference| symbols.get(reference))
        .map(String::as_str)
        .ok_or_else(|| {
            TsdbImportError::InvalidIndex(format!("symbol reference {reference} is out of range"))
        })
}
