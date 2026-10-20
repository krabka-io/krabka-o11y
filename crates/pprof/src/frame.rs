//! Resolved stack frames and symbol resolution boundary.

/// One resolved stack frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub function: String,
    pub file: String,
    pub line: i32,
}

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

/// Resolves a raw `(partition, stacktrace_id)` into frames.
pub trait SymbolSource: Send + Sync {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>;

    fn resolve_locations(&self, partition: u64, id: u32) -> Vec<ResolvedLocation> {
        self.resolve(partition, id)
            .into_iter()
            .map(ResolvedLocation::from)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    struct Fixed(Vec<Frame>);

    impl SymbolSource for Fixed {
        fn resolve(&self, _partition: u64, _id: u32) -> Vec<Frame> {
            self.0.clone()
        }
    }

    #[test]
    fn symbol_source_is_object_safe_and_returns_frames() {
        let src: Box<dyn SymbolSource> = Box::new(Fixed(vec![Frame {
            function: "main".to_string(),
            file: "main.go".to_string(),
            line: 10,
        }]));
        let frames = src.resolve(0, 1);
        assert!(frames.len() == 1);
        assert!(frames[0].function == "main");
    }
}
