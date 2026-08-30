use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StructuralOp {
    Descendant,
    Ancestor,
    Child,
    Parent,
    Sibling,
    NegDescendant,
    NegAncestor,
    NegChild,
    NegParent,
    UnionDescendant,
    UnionAncestor,
    UnionChild,
    UnionParent,
    UnionSibling,
}
