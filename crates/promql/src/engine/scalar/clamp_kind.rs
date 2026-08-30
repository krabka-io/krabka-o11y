use super::*;

#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) enum ClampKind {
    Both,
    Min,
    Max,
}

#[cfg(test)]
impl ClampKind {
    pub(crate) fn argument_count(self) -> usize {
        match self {
            Self::Both => 3,
            Self::Min | Self::Max => 2,
        }
    }
}
