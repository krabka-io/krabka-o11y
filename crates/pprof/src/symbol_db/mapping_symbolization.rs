use super::{Deserialize, Serialize};

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MappingSymbolization(pub(crate) u8);

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct MappingSymbolizationParts {
    pub functions: bool,
    pub filenames: bool,
    pub line_numbers: bool,
    pub inline_frames: bool,
}

impl MappingSymbolization {
    pub(crate) const FUNCTIONS: u8 = 1;
    pub(crate) const FILENAMES: u8 = 1 << 1;
    pub(crate) const LINE_NUMBERS: u8 = 1 << 2;
    pub(crate) const INLINE_FRAMES: u8 = 1 << 3;

    #[must_use]
    pub fn from_parts(parts: MappingSymbolizationParts) -> Self {
        let mut flags = 0;
        if parts.functions {
            flags |= Self::FUNCTIONS;
        }
        if parts.filenames {
            flags |= Self::FILENAMES;
        }
        if parts.line_numbers {
            flags |= Self::LINE_NUMBERS;
        }
        if parts.inline_frames {
            flags |= Self::INLINE_FRAMES;
        }
        Self(flags)
    }

    #[must_use]
    pub fn has_functions(self) -> bool {
        self.0 & Self::FUNCTIONS != 0
    }

    #[must_use]
    pub fn has_filenames(self) -> bool {
        self.0 & Self::FILENAMES != 0
    }

    #[must_use]
    pub fn has_line_numbers(self) -> bool {
        self.0 & Self::LINE_NUMBERS != 0
    }

    #[must_use]
    pub fn has_inline_frames(self) -> bool {
        self.0 & Self::INLINE_FRAMES != 0
    }
}
