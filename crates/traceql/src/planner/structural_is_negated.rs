use super::StructuralOp;

pub(crate) fn structural_is_negated(op: StructuralOp) -> bool {
    matches!(
        op,
        StructuralOp::NegDescendant
            | StructuralOp::NegAncestor
            | StructuralOp::NegChild
            | StructuralOp::NegParent
    )
}
