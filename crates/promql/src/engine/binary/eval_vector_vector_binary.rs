use super::*;

pub(crate) fn eval_vector_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Vec<InstantSample>> {
    let card = modifier.map_or(VectorMatchCardinality::OneToOne, |modifier| {
        modifier.card.clone()
    });
    match card {
        VectorMatchCardinality::OneToOne => {
            eval_one_to_one_vector_binary(left, right, op, modifier)
        }
        VectorMatchCardinality::ManyToOne(group_labels) => {
            eval_many_to_one_vector_binary(left, right, op, modifier, &group_labels.labels)
        }
        VectorMatchCardinality::OneToMany(group_labels) => {
            eval_one_to_many_vector_binary(left, right, op, modifier, &group_labels.labels)
        }
        VectorMatchCardinality::ManyToMany => Err(PromqlError::Unsupported(
            "many-to-many vector matching is only valid for set operators".to_string(),
        )),
    }
}
