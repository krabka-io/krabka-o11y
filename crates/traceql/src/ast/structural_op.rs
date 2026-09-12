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
    NegSibling,
    UnionDescendant,
    UnionAncestor,
    UnionChild,
    UnionParent,
    UnionSibling,
}
