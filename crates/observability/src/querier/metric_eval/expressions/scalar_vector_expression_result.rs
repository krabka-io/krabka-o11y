use super::{BTreeMap, VectorScalarExpressionParser};

#[derive(Clone)]
pub(crate) enum ScalarVectorExpressionResult {
    Scalar {
        sample: String,
    },
    Vector {
        sample: Option<String>,
        metric: BTreeMap<String, String>,
    },
}

pub(crate) fn scalar_vector_expression_result(query: &str) -> Option<ScalarVectorExpressionResult> {
    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    let mut parser = VectorScalarExpressionParser::new(&query);
    let result = parser.parse_result()?;
    if parser.is_finished() {
        Some(result)
    } else {
        None
    }
}
