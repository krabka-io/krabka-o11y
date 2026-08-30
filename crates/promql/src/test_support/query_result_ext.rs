use super::*;

pub(crate) trait QueryResultExt {
    fn single(&self) -> &InstantSample;
    fn as_scalar(&self) -> f64;
    fn values_f64(&self) -> Vec<f64>;
    fn is_empty(&self) -> bool;
    fn iter(&self) -> std::slice::Iter<'_, InstantSample>;
    fn len(&self) -> usize;
}
