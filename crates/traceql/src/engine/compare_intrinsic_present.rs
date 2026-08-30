use super::*;

pub(crate) fn compare_intrinsic_present(row: &CompareRow, intrinsic: &Intrinsic) -> bool {
    match intrinsic {
        Intrinsic::Name => row.name.as_ref().is_some_and(|name| !name.is_empty()),
        Intrinsic::Status => row.status_code.is_some(),
        Intrinsic::StatusMessage => row
            .status_message
            .as_ref()
            .is_some_and(|msg| !msg.is_empty()),
        Intrinsic::Kind => row.kind.is_some(),
        Intrinsic::Duration => row.duration.is_some(),
        _ => false,
    }
}
