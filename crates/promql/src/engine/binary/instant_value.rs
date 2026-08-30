use super::InstantSample;

pub(crate) enum InstantValue {
    Scalar(f64),
    Vector(Vec<InstantSample>),
}

#[cfg(test)]
impl InstantValue {
    pub(crate) fn try_from_query(result: QueryResult) -> Result<Self> {
        match result {
            QueryResult::Scalar { value, .. } => Ok(Self::Scalar(value)),
            QueryResult::InstantVector(samples) => Ok(Self::Vector(samples)),
            QueryResult::RangeMatrix(_) => Err(PromqlError::Plan(
                "binary expression requires instant operands".to_string(),
            )),
            QueryResult::Str { .. } => Err(PromqlError::Plan(
                "binary expression does not support string operands".to_string(),
            )),
        }
    }
}
