use krabka_domain_macros::EnumName;

use super::{ClockWireError, Deserialize, Serialize, pb};

/// The quality of a GNSS position solution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, EnumName)]
#[enum_name(accessor = "as_label")]
pub enum GnssFix {
    /// The receiver has no fix.
    #[name(value = "none")]
    None,
    /// The receiver has a two-dimensional fix.
    #[name(value = "2d")]
    TwoD,
    /// The receiver has a three-dimensional fix.
    #[name(value = "3d")]
    ThreeD,
}

impl GnssFix {
    /// Every fix quality, in wire order.
    pub const ALL: [Self; 3] = [Self::None, Self::TwoD, Self::ThreeD];
}

/// Reads the GNSS fix quality, where the unspecified value means the receiver
/// reported none.
///
/// This is the one enum whose zero value is not a rejection. The fix quality is
/// a source-specific, nullable column, and every reading from a source other
/// than GNSS leaves it at zero by construction. `GNSS_FIX_NONE` already says
/// "the receiver has no fix", so the zero value stays free to mean "the agent
/// reported no fix quality".
pub(crate) fn gnss_fix(index: usize, value: i32) -> Result<Option<GnssFix>, ClockWireError> {
    match pb::clocks::GnssFix::try_from(value) {
        Ok(pb::clocks::GnssFix::None) => Ok(Some(GnssFix::None)),
        Ok(pb::clocks::GnssFix::TwoD) => Ok(Some(GnssFix::TwoD)),
        Ok(pb::clocks::GnssFix::ThreeD) => Ok(Some(GnssFix::ThreeD)),
        Ok(pb::clocks::GnssFix::Unspecified) => Ok(None),
        Err(_) => Err(ClockWireError::UnknownEnum {
            index,
            field: "gnss_fix",
            value,
        }),
    }
}
