use super::{FieldFilterExpression, PipelineStage, FieldFilterChain};

pub(crate) fn field_filter_expression_to_pipeline_stage(
    expression: FieldFilterExpression,
) -> PipelineStage {
    match expression {
        FieldFilterExpression::Filter(filter) => PipelineStage::FieldFilter(filter),
        FieldFilterExpression::Chain { first, rest } => {
            let first = match *first {
                FieldFilterExpression::Filter(filter) => filter,
                first => {
                    return PipelineStage::FieldFilterExpression(FieldFilterExpression::Chain {
                        first: Box::new(first),
                        rest,
                    });
                }
            };

            let mut flat_rest = Vec::new();
            for (op, expression) in rest {
                let filter = match expression {
                    FieldFilterExpression::Filter(filter) => filter,
                    expression => {
                        let first = Box::new(FieldFilterExpression::Filter(first));
                        let rest = flat_rest
                            .into_iter()
                            .map(|(op, filter)| (op, FieldFilterExpression::Filter(filter)))
                            .chain(std::iter::once((op, expression)))
                            .collect();
                        return PipelineStage::FieldFilterExpression(
                            FieldFilterExpression::Chain { first, rest },
                        );
                    }
                };
                flat_rest.push((op, filter));
            }

            PipelineStage::FieldFilterChain(FieldFilterChain::new(first, flat_rest))
        }
        expression @ FieldFilterExpression::Group(_) => {
            PipelineStage::FieldFilterExpression(expression)
        }
    }
}
