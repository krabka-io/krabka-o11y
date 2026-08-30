use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MetricValue {
    pub(crate) numerator: i128,
    pub(crate) denominator: u128,
}
