use super::*;

pub(crate) fn intern_symbols(
    symdb: &mut SymbolDb,
    symbols: &WalSymbolSet,
) -> Result<SymbolRefs, ProfilesError> {
    let strings = symbols
        .strings
        .iter()
        .map(|value| symdb.intern_string(value))
        .collect::<Vec<_>>();

    let mappings = symbols
        .mappings
        .iter()
        .map(|mapping| symdb.intern_mapping(mapping_rec(mapping, &strings)))
        .collect::<Vec<_>>();

    let functions = symbols
        .functions
        .iter()
        .map(|function| {
            symdb.intern_function(FunctionRec {
                name: remap_ref(function.name, &strings),
                system_name: remap_ref(function.system_name, &strings),
                filename: remap_ref(function.filename, &strings),
                start_line: function.start_line,
            })
        })
        .collect::<Vec<_>>();

    let locations = symbols
        .locations
        .iter()
        .map(|location| {
            let location = LocationRec {
                address: location.address,
                mapping_id: remap_ref(location.mapping_id, &mappings),
                lines: location
                    .lines
                    .iter()
                    .map(|(function_id, line)| {
                        Ok(LineRec {
                            function_id: remap_ref(*function_id, &functions),
                            line: i32::try_from(*line).map_err(|err| {
                                ProfilesError::Block(format!("line number does not fit i32: {err}"))
                            })?,
                        })
                    })
                    .collect::<Result<Vec<_>, ProfilesError>>()?,
            };
            Ok(symdb.intern_location(location))
        })
        .collect::<Result<Vec<_>, ProfilesError>>()?;

    Ok(SymbolRefs { locations })
}
