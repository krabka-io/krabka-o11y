use super::{ClockSyncState, ClockWireError, pb};

pub(crate) fn sync_state(index: usize, value: i32) -> Result<ClockSyncState, ClockWireError> {
    match pb::clocks::SyncState::try_from(value) {
        Ok(pb::clocks::SyncState::Synchronized) => Ok(ClockSyncState::Synchronized),
        Ok(pb::clocks::SyncState::Holdover) => Ok(ClockSyncState::Holdover),
        Ok(pb::clocks::SyncState::FreeRunning) => Ok(ClockSyncState::FreeRunning),
        Ok(pb::clocks::SyncState::Unsynchronized) => Ok(ClockSyncState::Unsynchronized),
        Ok(pb::clocks::SyncState::Stepped) => Ok(ClockSyncState::Stepped),
        Ok(pb::clocks::SyncState::Unspecified) => Err(ClockWireError::UnspecifiedEnum {
            index,
            field: "sync_state",
        }),
        Err(_) => Err(ClockWireError::UnknownEnum {
            index,
            field: "sync_state",
            value,
        }),
    }
}
