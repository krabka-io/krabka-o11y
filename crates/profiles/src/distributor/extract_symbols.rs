use super::{
    HashMap, PprofProfile, ProfilesError, WalFunction, WalLocation, WalMapping, WalSymbolSet,
    normalize_optional_pprof_id, normalize_required_pprof_id, u32_from_i64,
};

pub(crate) fn extract_symbols(profile: &PprofProfile) -> Result<WalSymbolSet, ProfilesError> {
    let inner = profile.inner();
    let function_refs = inner
        .function
        .iter()
        .enumerate()
        .map(|(idx, function)| {
            let idx = u32::try_from(idx).map_err(|err| {
                ProfilesError::Decode(format!("function index does not fit u32: {err}"))
            })?;
            Ok((function.id, idx))
        })
        .collect::<Result<HashMap<_, _>, ProfilesError>>()?;
    let mapping_refs = inner
        .mapping
        .iter()
        .enumerate()
        .map(|(idx, mapping)| {
            let idx = u32::try_from(idx).map_err(|err| {
                ProfilesError::Decode(format!("mapping index does not fit u32: {err}"))
            })?;
            Ok((mapping.id, idx))
        })
        .collect::<Result<HashMap<_, _>, ProfilesError>>()?;
    Ok(WalSymbolSet {
        strings: inner.string_table.clone(),
        functions: inner
            .function
            .iter()
            .map(|function| {
                Ok(WalFunction {
                    name: u32_from_i64(function.name, "function.name")?,
                    system_name: u32_from_i64(function.system_name, "function.system_name")?,
                    filename: u32_from_i64(function.filename, "function.filename")?,
                    start_line: function.start_line,
                })
            })
            .collect::<Result<Vec<_>, ProfilesError>>()?,
        locations: inner
            .location
            .iter()
            .map(|location| {
                Ok(WalLocation {
                    address: location.address,
                    mapping_id: normalize_optional_pprof_id(
                        location.mapping_id,
                        &mapping_refs,
                        "location.mapping_id",
                    )?,
                    lines: location
                        .line
                        .iter()
                        .map(|line| {
                            Ok((
                                normalize_required_pprof_id(
                                    line.function_id,
                                    &function_refs,
                                    "line.function_id",
                                )?,
                                line.line,
                            ))
                        })
                        .collect::<Result<Vec<_>, ProfilesError>>()?,
                })
            })
            .collect::<Result<Vec<_>, ProfilesError>>()?,
        mappings: inner
            .mapping
            .iter()
            .map(|mapping| {
                Ok(WalMapping {
                    memory_start: mapping.memory_start,
                    memory_limit: mapping.memory_limit,
                    file_offset: mapping.file_offset,
                    filename: u32_from_i64(mapping.filename, "mapping.filename")?,
                    build_id: u32_from_i64(mapping.build_id, "mapping.build_id")?,
                    // Carry each pprof symbolization flag through independently;
                    // they are distinct signals (functions vs filenames vs line
                    // numbers vs inline frames) and must not be collapsed.
                    has_functions: mapping.symbolization.has_functions().into(),
                    has_filenames: mapping.symbolization.has_filenames().into(),
                    has_line_numbers: mapping.symbolization.has_line_numbers().into(),
                    has_inline_frames: mapping.symbolization.has_inline_frames().into(),
                })
            })
            .collect::<Result<Vec<_>, ProfilesError>>()?,
    })
}
