use super::{ClockSourceKind, ClockWireError, pb};

pub(crate) fn source_kind(index: usize, value: i32) -> Result<ClockSourceKind, ClockWireError> {
    match pb::clocks::SourceKind::try_from(value) {
        Ok(pb::clocks::SourceKind::Ptp) => Ok(ClockSourceKind::Ptp),
        Ok(pb::clocks::SourceKind::Ntp) => Ok(ClockSourceKind::Ntp),
        Ok(pb::clocks::SourceKind::Gnss) => Ok(ClockSourceKind::Gnss),
        Ok(pb::clocks::SourceKind::KernelTimex) => Ok(ClockSourceKind::KernelTimex),
        Ok(pb::clocks::SourceKind::Phc) => Ok(ClockSourceKind::Phc),
        Ok(pb::clocks::SourceKind::Unspecified) => Err(ClockWireError::UnspecifiedEnum {
            index,
            field: "source_kind",
        }),
        Err(_) => Err(ClockWireError::UnknownEnum {
            index,
            field: "source_kind",
            value,
        }),
    }
}
