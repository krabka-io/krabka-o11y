use super::StructuralOp;

pub(crate) fn structural_is_union(op: StructuralOp) -> bool {
    matches!(
        op,
        StructuralOp::UnionDescendant
            | StructuralOp::UnionAncestor
            | StructuralOp::UnionChild
            | StructuralOp::UnionParent
            | StructuralOp::UnionSibling
    )
}
