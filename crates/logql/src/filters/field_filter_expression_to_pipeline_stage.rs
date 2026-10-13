use super::{FieldFilterChain, FieldFilterExpression, FieldFilterStep, PipelineStage};

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

            let mut flat_rest: Vec<FieldFilterStep<super::FieldFilter>> = Vec::new();
            for step in rest {
                let operation = step.operation;
                let filter = match step.filter {
                    FieldFilterExpression::Filter(filter) => filter,
                    expression => {
                        let first = Box::new(FieldFilterExpression::Filter(first));
                        let rest = flat_rest
                            .into_iter()
                            .map(|step| FieldFilterStep {
                                operation: step.operation,
                                filter: FieldFilterExpression::Filter(step.filter),
                            })
                            .chain(std::iter::once(FieldFilterStep {
                                operation,
                                filter: expression,
                            }))
                            .collect();
                        return PipelineStage::FieldFilterExpression(
                            FieldFilterExpression::Chain { first, rest },
                        );
                    }
                };
                flat_rest.push(FieldFilterStep { operation, filter });
            }

            PipelineStage::FieldFilterChain(FieldFilterChain::new(first, flat_rest))
        }
        expression @ FieldFilterExpression::Group(_) => {
            PipelineStage::FieldFilterExpression(expression)
        }
    }
}
