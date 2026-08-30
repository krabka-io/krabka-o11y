use super::ScanOptions;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchOptions {
    pub limit: usize,
    pub spss: usize,
    pub search_limit: Option<usize>,
    pub scan_options: ScanOptions,
}
