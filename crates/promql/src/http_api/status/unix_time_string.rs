use super::*;

pub(crate) fn unix_time_string(time: SystemTime) -> String {
    time.duration_since(UNIX_EPOCH).map_or_else(
        |_| "0".to_string(),
        |duration| duration.as_secs().to_string(),
    )
}
