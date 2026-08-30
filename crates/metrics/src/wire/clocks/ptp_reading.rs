use super::{Deserialize, Serialize, i64};

/// The PTP measurements from `pmc GET TIME_STATUS_NP`, `CURRENT_DATA_SET`, and
/// `PARENT_DATA_SET`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PtpReading {
    /// The one-way path delay the port measures to the master.
    pub mean_path_delay_nanos: i64,
    /// The count of boundary clocks between this port and the grandmaster.
    pub steps_removed: u32,
    /// The grandmaster `clockClass`, which states how traceable its time is.
    pub gm_clock_class: u32,
    /// The grandmaster `clockAccuracy`, which states the expected error of its
    /// time.
    pub gm_clock_accuracy: u32,
}
