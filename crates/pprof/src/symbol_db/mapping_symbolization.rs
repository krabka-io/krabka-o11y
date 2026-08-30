use super::{Deserialize, Serialize};

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MappingSymbolization(pub(crate) u8);

impl MappingSymbolization {
    pub(crate) const FUNCTIONS: u8 = 1;
    pub(crate) const FILENAMES: u8 = 1 << 1;
    pub(crate) const LINE_NUMBERS: u8 = 1 << 2;
    pub(crate) const INLINE_FRAMES: u8 = 1 << 3;

    #[must_use]
    pub fn from_parts(parts: (bool, bool, bool, bool)) -> Self {
        let (has_functions, has_filenames, has_line_numbers, has_inline_frames) = parts;
        let mut flags = 0;
        if has_functions {
            flags |= Self::FUNCTIONS;
        }
        if has_filenames {
            flags |= Self::FILENAMES;
        }
        if has_line_numbers {
            flags |= Self::LINE_NUMBERS;
        }
        if has_inline_frames {
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
