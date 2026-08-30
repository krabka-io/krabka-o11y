use super::{ResetHint, pb};

pub(crate) fn v2_reset_hint(value: i32) -> ResetHint {
    match pb::v2::histogram::ResetHint::try_from(value) {
        Ok(pb::v2::histogram::ResetHint::Yes) => ResetHint::Yes,
        Ok(pb::v2::histogram::ResetHint::No) => ResetHint::No,
        Ok(pb::v2::histogram::ResetHint::Gauge) => ResetHint::Gauge,
        Ok(pb::v2::histogram::ResetHint::Unspecified) | Err(_) => ResetHint::Unknown,
    }
}
