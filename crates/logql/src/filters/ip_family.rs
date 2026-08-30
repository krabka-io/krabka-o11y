
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IpFamily {
    V4,
    V6,
}

impl IpFamily {
    pub(crate) fn bits(self) -> u8 {
        match self {
            Self::V4 => 32,
            Self::V6 => 128,
        }
    }

    pub(crate) fn mask(self) -> u128 {
        match self {
            Self::V4 => u128::from(u32::MAX),
            Self::V6 => u128::MAX,
        }
    }
}
