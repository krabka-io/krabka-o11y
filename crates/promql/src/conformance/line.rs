#[derive(Clone, Copy)]
pub(crate) struct Line<'a> {
    pub(crate) number: usize,
    pub(crate) raw: &'a str,
    pub(crate) trimmed: &'a str,
}
