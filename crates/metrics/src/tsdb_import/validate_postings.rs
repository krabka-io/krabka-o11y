use super::{BTreeMap, ByteReader, IndexSeries, IndexToc, TsdbImportError, checked_section};

/// Checks the postings offset table and every postings list against the
/// series section.
///
/// The import reads series from the series section alone, but Prometheus
/// answers selectors from the postings. A block whose postings disagree with
/// its series would query differently in Prometheus and in Krabka, so the
/// import refuses it. The table must list each label pair once, in sorted
/// order, starting with the all-series list under the empty pair, and each
/// list must name exactly the series that carry the pair.
pub fn validate_postings(
    index: &[u8],
    toc: &IndexToc,
    series: &[IndexSeries],
) -> Result<(), TsdbImportError> {
    let mut expected = BTreeMap::<(&str, &str), Vec<u32>>::new();
    for entry in series {
        let reference = u32::try_from(entry.reference).map_err(|_| {
            TsdbImportError::InvalidIndex(format!(
                "series reference {} overflows 32 bits",
                entry.reference
            ))
        })?;
        expected.entry(("", "")).or_default().push(reference);
        for (name, value) in &entry.labels {
            expected
                .entry((name.as_str(), value.as_str()))
                .or_default()
                .push(reference);
        }
    }

    let table = checked_section(index, toc.postings_table, "index postings offset table")?;
    let mut reader = ByteReader::new(table, "index postings offset table");
    let count = usize::try_from(reader.be32()?).unwrap_or(usize::MAX);
    if count != expected.len() {
        return Err(TsdbImportError::InvalidIndex(format!(
            "postings offset table lists {count} label pairs, series carry {}",
            expected.len()
        )));
    }
    for ((name, value), references) in &expected {
        if reader.uvarint()? != 2 {
            return Err(TsdbImportError::InvalidIndex(
                "postings offset table entry does not hold a label pair".to_owned(),
            ));
        }
        let found_name = reader.uvarint_bytes()?;
        let found_value = reader.uvarint_bytes()?;
        if found_name != name.as_bytes() || found_value != value.as_bytes() {
            return Err(TsdbImportError::InvalidIndex(format!(
                "postings offset table does not list {name}={value:?} in order"
            )));
        }
        let offset = reader.uvarint()?;
        check_list(index, toc, offset, name, value, references)?;
    }
    if reader.remaining() != 0 {
        return Err(TsdbImportError::InvalidIndex(
            "postings offset table holds bytes after its last entry".to_owned(),
        ));
    }
    Ok(())
}

fn check_list(
    index: &[u8],
    toc: &IndexToc,
    offset: u64,
    name: &str,
    value: &str,
    references: &[u32],
) -> Result<(), TsdbImportError> {
    if offset < toc.postings || offset >= toc.label_indices_table || !offset.is_multiple_of(4) {
        return Err(TsdbImportError::InvalidIndex(format!(
            "postings list for {name}={value:?} starts at {offset}, outside the postings section"
        )));
    }
    let end = usize::try_from(toc.label_indices_table).unwrap_or(usize::MAX);
    let start = usize::try_from(offset).unwrap_or(usize::MAX);
    let section = index.get(start..end).ok_or(TsdbImportError::Truncated {
        section: "index postings",
    })?;
    let mut reader = ByteReader::new(section, "index postings");
    let length = usize::try_from(reader.be32()?).unwrap_or(usize::MAX);
    let content = reader.bytes(length)?;
    let crc = reader.be32()?;
    if crc32c::crc32c(content) != crc {
        return Err(TsdbImportError::Checksum {
            section: format!("postings list for {name}={value:?}"),
        });
    }
    let mut list = ByteReader::new(content, "index postings");
    let count = usize::try_from(list.be32()?).unwrap_or(usize::MAX);
    if count != references.len() || list.remaining() != count.saturating_mul(4) {
        return Err(TsdbImportError::InvalidIndex(format!(
            "postings list for {name}={value:?} does not match the series that carry it"
        )));
    }
    for reference in references {
        if list.be32()? != *reference {
            return Err(TsdbImportError::InvalidIndex(format!(
                "postings list for {name}={value:?} does not match the series that carry it"
            )));
        }
    }
    Ok(())
}
