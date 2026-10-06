use super::{
    ByteReader, IndexToc, MetricString, TsdbImportError, TsdbImportLimits, checked_section,
};

/// Reads the symbol table. Symbols retain arbitrary bytes, and the table
/// must be strictly sorted by bytes, as the Prometheus writer produces it.
pub fn read_symbols(
    index: &[u8],
    toc: &IndexToc,
    limits: &TsdbImportLimits,
) -> Result<Vec<MetricString>, TsdbImportError> {
    let content = checked_section(index, toc.symbols, "index symbols")?;
    let mut reader = ByteReader::new(content, "index symbols");
    let count = u64::from(reader.be32()?);
    TsdbImportLimits::check("symbols", count, limits.max_symbols)?;
    let count = usize::try_from(count)
        .ok()
        .filter(|count| *count <= reader.remaining())
        .ok_or(TsdbImportError::Truncated {
            section: "index symbols",
        })?;
    let mut symbols = Vec::<MetricString>::with_capacity(count);
    for _ in 0..count {
        let symbol = MetricString::from(reader.uvarint_bytes()?.to_vec());
        if symbols.last().is_some_and(|last| last >= &symbol) {
            return Err(TsdbImportError::InvalidIndex(format!(
                "symbol {} is out of order",
                symbol.quoted()
            )));
        }
        symbols.push(symbol);
    }
    if reader.remaining() != 0 {
        return Err(TsdbImportError::InvalidIndex(
            "symbol table holds bytes after its last symbol".to_owned(),
        ));
    }
    Ok(symbols)
}
