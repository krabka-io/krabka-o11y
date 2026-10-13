use super::{MappingRec, MappingSymbolization, WalMapping, remap_ref};

pub(crate) fn mapping_rec(mapping: &WalMapping, strings: &[u32]) -> MappingRec {
    MappingRec {
        memory_start: mapping.memory_start,
        memory_limit: mapping.memory_limit,
        file_offset: mapping.file_offset,
        filename: remap_ref(mapping.filename, strings),
        build_id: remap_ref(mapping.build_id, strings),
        symbolization: MappingSymbolization::from_parts(krabka_pprof::MappingSymbolizationParts {
            functions: mapping.has_functions.get(),
            filenames: mapping.has_filenames.get(),
            line_numbers: mapping.has_line_numbers.get(),
            inline_frames: mapping.has_inline_frames.get(),
        }),
    }
}
