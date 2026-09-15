use super::Frame;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResolvedFunction {
    pub name: String,
    pub system_name: String,
    pub filename: String,
    pub start_line: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResolvedLine {
    pub function: ResolvedFunction,
    pub line: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(clippy::struct_excessive_bools)]
pub struct ResolvedMapping {
    pub memory_start: u64,
    pub memory_limit: u64,
    pub file_offset: u64,
    pub filename: String,
    pub build_id: String,
    pub has_functions: bool,
    pub has_filenames: bool,
    pub has_line_numbers: bool,
    pub has_inline_frames: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResolvedLocation {
    pub address: u64,
    pub mapping: Option<ResolvedMapping>,
    pub lines: Vec<ResolvedLine>,
}

impl From<Frame> for ResolvedLocation {
    fn from(frame: Frame) -> Self {
        Self {
            address: 0,
            mapping: None,
            lines: vec![ResolvedLine {
                function: ResolvedFunction {
                    name: frame.function.clone(),
                    system_name: frame.function,
                    filename: frame.file,
                    start_line: 0,
                },
                line: i64::from(frame.line),
            }],
        }
    }
}
