//! Nested-set interval assignment for one trace's span forest.

use std::collections::HashMap;

use super::Span;

#[cfg(test)]
mod tests {

    use super::*;
    use crate::span::{AttrValue, KeyValue, SpanKind, StatusCode};

    fn span(id: u8, parent: Option<u8>) -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: format!("s{id}"),
            kind: SpanKind::Internal,
            start_ns: 0,
            duration_ns: 1,
            status: StatusCode::Unset,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: Vec::new(),
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        }
    }

    #[test]
    fn ancestor_interval_contains_descendants() {
        let spans = vec![
            span(1, None),
            span(2, Some(1)),
            span(3, Some(2)),
            span(4, Some(1)),
        ];
        let ns = assign_nested_set(&spans);
        let root = ns[0];
        for child in &ns[1..] {
            assert2::assert!(child.left > root.left);
            assert2::assert!(child.right < root.right);
        }
        // Pre-order intervals: span 3 nests inside span 2, while the sibling
        // span 4 falls outside span 2's interval.
        assert2::assert!(
            ns == vec![
                NestedSet {
                    left: 1,
                    right: 8,
                    parent_id: -1
                },
                NestedSet {
                    left: 2,
                    right: 5,
                    parent_id: 1
                },
                NestedSet {
                    left: 3,
                    right: 4,
                    parent_id: 2
                },
                NestedSet {
                    left: 6,
                    right: 7,
                    parent_id: 1
                },
            ]
        );
    }

    #[test]
    fn child_parent_id_equals_parent_left() {
        let spans = vec![span(1, None), span(2, Some(1))];
        let ns = assign_nested_set(&spans);
        assert2::assert!(ns[1].parent_id == ns[0].left);
    }

    #[test]
    fn roots_have_negative_one_parent_id() {
        // A span with no parent, and one whose parent_span_id is dangling
        // (parent not in the batch), are both roots: nestedSetParent = -1,
        // matching Tempo so `nestedSetParent < 0` selects them.
        let spans = vec![span(1, None), span(2, Some(99))];
        let ns = assign_nested_set(&spans);
        assert2::assert!(ns[0].parent_id == -1);
        assert2::assert!(ns[1].parent_id == -1);
    }

    #[test]
    fn every_interval_has_left_before_right() {
        let spans = vec![span(1, None), span(2, Some(1)), span(3, Some(1))];
        for ns in assign_nested_set(&spans) {
            assert2::assert!(ns.left < ns.right);
        }
    }
}

mod assign_nested_set;
mod nested_set_type;

pub use assign_nested_set::assign_nested_set;
pub use nested_set_type::NestedSet;
