use super::*;

pub(crate) fn mapping_rec(mapping: &WalMapping, strings: &[u32]) -> MappingRec {
    MappingRec {
        memory_start: mapping.memory_start,
        memory_limit: mapping.memory_limit,
        file_offset: mapping.file_offset,
        filename: remap_ref(mapping.filename, strings),
        build_id: remap_ref(mapping.build_id, strings),
        symbolization: MappingSymbolization::from_parts((
            mapping.has_functions.get(),
            mapping.has_filenames.get(),
            mapping.has_line_numbers.get(),
            mapping.has_inline_frames.get(),
        )),
    }
}
