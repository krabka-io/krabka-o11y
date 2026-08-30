use super::{ScanOptions, UnixNano};

pub(crate) struct PlannerContext {
    pub tenant: String,
    pub start_ns: UnixNano,
    pub end_ns: UnixNano,
    pub scan_options: ScanOptions,
}
