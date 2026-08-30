use super::{
    ProfileRecord, ProfilesError, STACKTRACE_PARTITION, SymbolDb, intern_symbols, remap_ref,
};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn intern_record(symdb: &mut SymbolDb, rec: &ProfileRecord) -> Result<Vec<u32>, ProfilesError> {
    let symbols = intern_symbols(symdb, &rec.symbols)?;
    rec.samples
        .iter()
        .map(|sample| {
            let stack = sample
                .stacktrace_location_refs
                .iter()
                .map(|location_ref| remap_ref(*location_ref, &symbols.locations))
                .collect::<Vec<_>>();
            Ok(symdb.intern_stacktrace(STACKTRACE_PARTITION, &stack))
        })
        .collect()
}
