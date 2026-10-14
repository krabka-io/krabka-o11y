#![allow(dead_code)] // These fields mirror the wire format consumed by serde.

/// The shape of a profiles `.symdb` symbol table, as `krabka-pprof` persists
/// it in `SymbolDb`.
///
/// The codec is `serde-wincode`, which writes a struct's fields in order.
/// The fields of
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
#[derive(serde::Deserialize)]
pub struct SymbolTableShape {
    pub strings: Vec<String>,
    pub functions: Vec<FunctionRecord>,
    pub locations: Vec<LocationRecord>,
    pub mappings: Vec<MappingRecord>,
    pub partitions: std::collections::HashMap<u64, SymbolPartition>,
}

#[derive(serde::Deserialize)]
pub struct FunctionRecord {
    pub name: u32,
    pub system_name: u32,
    pub filename: u32,
    pub start_line: i64,
}

#[derive(serde::Deserialize)]
pub struct LocationRecord {
    pub address: u64,
    pub mapping_id: u32,
    pub lines: Vec<LineRecord>,
}

#[derive(serde::Deserialize)]
pub struct LineRecord {
    pub function_id: u32,
    pub line: i32,
}

#[derive(serde::Deserialize)]
pub struct MappingRecord {
    pub memory_start: u64,
    pub memory_limit: u64,
    pub file_offset: u64,
    pub filename: u32,
    pub build_id: u32,
    pub symbolization: u8,
}

#[derive(serde::Deserialize)]
pub struct SymbolPartition {
    pub nodes: Vec<TreeNode>,
}

#[derive(serde::Deserialize)]
pub struct TreeNode {
    pub parent: i32,
    pub location_ref: i32,
}
