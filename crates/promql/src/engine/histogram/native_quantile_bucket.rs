
#[derive(Clone, Copy)]
pub(crate) struct NativeQuantileBucket {
    pub(crate) lower: f64,
    pub(crate) upper: f64,
    pub(crate) count: f64,
}
