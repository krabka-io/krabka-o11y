use super::{ByteReader, IndexToc, TsdbImportError, TsdbImportLimits, checked_section};

/// Reads the symbol table. Every symbol must be valid UTF-8, and the table
/// must be strictly sorted, as the Prometheus writer produces it.
pub fn read_symbols(
    index: &[u8],
    toc: &IndexToc,
    limits: &TsdbImportLimits,
) -> Result<Vec<String>, TsdbImportError> {
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
    let mut symbols = Vec::<String>::with_capacity(count);
    for _ in 0..count {
        let symbol = std::str::from_utf8(reader.uvarint_bytes()?).map_err(|_| {
            TsdbImportError::InvalidLabels("a symbol is not valid UTF-8".to_owned())
        })?;
        if symbols.last().is_some_and(|last| last.as_str() >= symbol) {
            return Err(TsdbImportError::InvalidIndex(format!(
                "symbol {symbol:?} is out of order"
            )));
        }
        symbols.push(symbol.to_owned());
    }
    if reader.remaining() != 0 {
        return Err(TsdbImportError::InvalidIndex(
            "symbol table holds bytes after its last symbol".to_owned(),
        ));
    }
    Ok(symbols)
}
