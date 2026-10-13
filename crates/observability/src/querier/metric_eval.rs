use crate::MetricValue;

pub(crate) struct MetricOperandValues {
    pub(crate) left: MetricValue,
    pub(crate) right: MetricValue,
}

pub(crate) mod binary_arithmetic;
pub(crate) mod binary_sets;
pub(crate) mod expression_parser;
pub(crate) mod expressions;
pub(crate) mod http_queries;
pub(crate) mod result_transforms;
pub(crate) mod scalar_samples;
pub(crate) mod validation;
