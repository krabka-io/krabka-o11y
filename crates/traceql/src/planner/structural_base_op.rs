use super::*;

pub(crate) fn structural_base_op(op: StructuralOp) -> StructuralOp {
    match op {
        StructuralOp::NegDescendant | StructuralOp::UnionDescendant => StructuralOp::Descendant,
        StructuralOp::NegAncestor | StructuralOp::UnionAncestor => StructuralOp::Ancestor,
        StructuralOp::NegChild | StructuralOp::UnionChild => StructuralOp::Child,
        StructuralOp::NegParent | StructuralOp::UnionParent => StructuralOp::Parent,
        StructuralOp::UnionSibling => StructuralOp::Sibling,
        StructuralOp::Descendant
        | StructuralOp::Ancestor
        | StructuralOp::Child
        | StructuralOp::Parent
        | StructuralOp::Sibling => op,
    }
}
