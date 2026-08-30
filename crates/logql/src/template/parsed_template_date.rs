#[derive(Clone, Copy, Debug)]
pub(crate) struct ParsedTemplateDate {
    pub(crate) year: i32,
    pub(crate) month: u32,
    pub(crate) day: u32,
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
    pub(crate) nanosecond: u32,
    pub(crate) offset_seconds: Option<i32>,
}
