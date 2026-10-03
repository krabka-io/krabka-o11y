/// The shape of a profiles `.symdb` symbol table, as `krabka-pprof` persists
/// it in `SymbolDb`.
///
/// The codec is `serde-wincode`, which writes a struct as its fields in
/// order, so each struct here is a tuple of its fields. The fields of
/// `SymbolDb` that serde skips are not in the bytes. In order:
///
/// - `strings`;
/// - `functions`, each `FunctionRec { name, system_name, filename, start_line }`;
/// - `locations`, each `LocationRec { address, mapping_id, lines }`, with
///   each line a `LineRec { function_id, line }`;
/// - `mappings`, each `MappingRec { memory_start, memory_limit, file_offset,
///   filename, build_id, symbolization }`;
/// - `partitions`, each a `Partition { nodes }` of `TreeNode { parent,
///   location_ref }`.
pub type SymbolTableShape = (
    Vec<String>,
    Vec<(u32, u32, u32, i64)>,
    Vec<(u64, u32, Vec<(u32, i32)>)>,
    Vec<(u64, u64, u64, u32, u32, u8)>,
    std::collections::HashMap<u64, Vec<(i32, i32)>>,
);
