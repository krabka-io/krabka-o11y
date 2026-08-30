use super::*;

pub(crate) fn v1_reset_hint(value: i32) -> ResetHint {
    match pb::v1::histogram::ResetHint::try_from(value) {
        Ok(pb::v1::histogram::ResetHint::Yes) => ResetHint::Yes,
        Ok(pb::v1::histogram::ResetHint::No) => ResetHint::No,
        Ok(pb::v1::histogram::ResetHint::Gauge) => ResetHint::Gauge,
        Ok(pb::v1::histogram::ResetHint::Unknown) | Err(_) => ResetHint::Unknown,
    }
}
