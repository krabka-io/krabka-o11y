use super::{
    InstantSample, PromqlError, Result, VectorMatchCardinality, VectorMatching, VectorOperands,
    eval_many_to_one_vector_binary, eval_one_to_many_vector_binary, eval_one_to_one_vector_binary,
};

pub(crate) fn eval_vector_vector_binary(
    operands: VectorOperands,
    matching: VectorMatching<'_>,
) -> Result<Vec<InstantSample>> {
    let modifier = matching.modifier;
    let card = modifier.map_or(VectorMatchCardinality::OneToOne, |modifier| {
        modifier.card.clone()
    });
    match card {
        VectorMatchCardinality::OneToOne => eval_one_to_one_vector_binary(operands, matching),
        VectorMatchCardinality::ManyToOne(group_labels) => {
            eval_many_to_one_vector_binary(operands, matching, &group_labels.labels)
        }
        VectorMatchCardinality::OneToMany(group_labels) => {
            let VectorOperands { left, right } = operands;
            eval_one_to_many_vector_binary(left, right, matching.op, modifier, &group_labels.labels)
        }
        VectorMatchCardinality::ManyToMany => Err(PromqlError::Unsupported(
            "many-to-many vector matching is only valid for set operators".to_string(),
        )),
    }
}
