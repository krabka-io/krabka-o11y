use super::*;

pub(crate) fn structural_predicate_sql(op: StructuralOp) -> String {
    let trace = selector::ident(COL_TRACE_ID);
    let left = selector::ident(COL_NS_LEFT);
    let right = selector::ident(COL_NS_RIGHT);
    let parent = selector::ident(COL_PARENT_ID);
    let span_id = selector::ident(COL_SPAN_ID);
    let trace_eq = format!("b.{trace} = a.{trace}");
    match op {
        StructuralOp::Descendant => {
            format!("{trace_eq} AND b.{left} > a.{left} AND b.{right} < a.{right}")
        }
        StructuralOp::Ancestor => {
            format!("{trace_eq} AND b.{left} < a.{left} AND b.{right} > a.{right}")
        }
        StructuralOp::Child => format!("{trace_eq} AND b.{parent} = a.{left}"),
        StructuralOp::Parent => format!("{trace_eq} AND a.{parent} = b.{left}"),
        StructuralOp::Sibling => {
            format!("{trace_eq} AND b.{parent} = a.{parent} AND b.{span_id} != a.{span_id}")
        }
        StructuralOp::NegDescendant
        | StructuralOp::NegAncestor
        | StructuralOp::NegChild
        | StructuralOp::NegParent
        | StructuralOp::UnionDescendant
        | StructuralOp::UnionAncestor
        | StructuralOp::UnionChild
        | StructuralOp::UnionParent
        | StructuralOp::UnionSibling => unreachable!("mode variants are normalized first"),
    }
}
