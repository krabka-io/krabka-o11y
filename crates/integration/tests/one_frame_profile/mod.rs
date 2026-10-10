// The symbol table of a profile whose every sample is one frame.

use krabka_profiles::wal::{WalFunction, WalLocation, WalMapping, WalSymbolSet};

// One function named `frame`, at one location, in one unnamed mapping.
pub fn one_frame_symbols(frame: &str) -> WalSymbolSet {
    WalSymbolSet {
        strings: vec![String::new(), frame.to_string()],
        functions: vec![WalFunction {
            name: 1,
            system_name: 1,
            filename: 0,
            start_line: 0,
        }],
        locations: vec![WalLocation {
            address: 0x1000,
            mapping_id: 0,
            lines: vec![(0, 10)],
        }],
        mappings: vec![WalMapping {
            memory_start: 0,
            memory_limit: 0,
            file_offset: 0,
            filename: 0,
            build_id: 0,
            has_functions: true.into(),
            has_filenames: false.into(),
            has_line_numbers: false.into(),
            has_inline_frames: false.into(),
        }],
    }
}
