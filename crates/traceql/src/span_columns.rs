//! `TraceQL` span column names and structural interval helpers.

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krabka_units::Time;

use crate::result::{AttrValue, EventRef, LinkRef};

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::{assert, check};
    use krabka_units::nanos;

    use super::*;

    fn sid(n: u8) -> [u8; 8] {
        [n, 0, 0, 0, 0, 0, 0, 0]
    }

    fn span(id: u8, parent: Option<u8>) -> InputSpan {
        InputSpan {
            trace_id: [7; 16],
            span_id: sid(id),
            parent_span_id: parent.map(sid),
            name: format!("span-{id}"),
            kind: 0,
            start_unix_nano: i64::from(id) * 100,
            duration: nanos(10),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            attrs: vec![],
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    fn idx(spans: &[InputSpan], id: u8) -> usize {
        spans.iter().position(|s| s.span_id == sid(id)).unwrap()
    }

    #[test]
    fn schema_contains_traceql_planning_columns() {
        let schema = span_schema();
        for (col, want) in [
            (COL_TRACE_ID, DataType::FixedSizeBinary(16)),
            (COL_NAME, DataType::Utf8),
            (COL_NS_LEFT, DataType::Int32),
            (COL_CHILD_COUNT, DataType::Int32),
            (COL_TRACE_DURATION, DataType::Int64),
        ] {
            assert!(
                schema.column_with_name(col).unwrap().1.data_type() == &want,
                "column: {col}"
            );
        }
    }

    #[test]
    fn attr_prefix_matches_tempo_virtual_attribute_shape() {
        assert!(ATTR_PREFIX == "attr.");
    }

    #[test]
    fn nested_set_parent_id_is_parent_left() {
        let spans = vec![
            span(1, None),
            span(2, Some(1)),
            span(3, Some(1)),
            span(4, Some(3)),
        ];
        let ns = assign_nested_set(&spans);
        let root_left = ns[idx(&spans, 1)].left;
        check!(ns[idx(&spans, 1)].parent_id == -1); // root: Tempo nestedSetParent sentinel
        check!(ns[idx(&spans, 2)].parent_id == root_left);
        check!(ns[idx(&spans, 3)].parent_id == root_left);
        check!(ns[idx(&spans, 4)].parent_id == ns[idx(&spans, 3)].left);
    }

    #[test]
    fn nested_set_intervals_identify_ancestors() {
        let spans = vec![
            span(1, None),
            span(2, Some(1)),
            span(3, Some(1)),
            span(4, Some(3)),
        ];
        let ns = assign_nested_set(&spans);
        let root = ns[idx(&spans, 1)];
        let peer = ns[idx(&spans, 2)];
        let parent = ns[idx(&spans, 3)];
        let child = ns[idx(&spans, 4)];

        check!(root.left < child.left && child.right < root.right);
        check!(parent.left < child.left && child.right < parent.right);
        check!(!(peer.left < child.left && child.right < peer.right));
    }

    #[test]
    fn orphan_parent_is_treated_as_root() {
        let spans = vec![span(9, Some(99))];
        let ns = assign_nested_set(&spans);
        // dangling parent → root sentinel
        assert!(
            ns == vec![NestedSet {
                left: 1,
                right: 2,
                parent_id: -1,
            }]
        );
    }

    #[test]
    fn self_parented_span_keeps_input_order_root_position() {
        let spans = vec![span(1, Some(1)), span(2, None)];
        let ns = assign_nested_set(&spans);
        assert!(
            ns == vec![
                NestedSet {
                    left: 1,
                    right: 2,
                    parent_id: -1,
                },
                NestedSet {
                    left: 3,
                    right: 4,
                    parent_id: -1,
                },
            ]
        );
    }

    #[test]
    fn cyclic_parents_still_get_valid_intervals() {
        // A.parent = B and B.parent = A: neither is a root, so the DFS seeded
        // only from roots would never visit them and leave {left:0,right:0},
        // colliding with real roots. Every node must still get left < right.
        let spans = vec![span(1, Some(2)), span(2, Some(1))];
        let ns = assign_nested_set(&spans);
        for entry in &ns {
            assert!(entry.left > 0);
            assert!(entry.left < entry.right);
        }
        check!(ns[0].parent_id == -1);
        check!(ns[1].parent_id == ns[0].left);
        check!(ns[0].left < ns[1].left && ns[1].right < ns[0].right);
        // The two intervals must be distinct (no collision at 0).
        check!(ns[0].left != ns[1].left);
    }

    #[test]
    fn normal_forest_unchanged_by_cycle_sweep() {
        let spans = vec![
            span(1, None),
            span(2, Some(1)),
            span(3, Some(1)),
            span(4, Some(3)),
        ];
        let ns = assign_nested_set(&spans);
        // Pre-existing well-formed assignment is preserved.
        assert!(ns[idx(&spans, 1)].left == 1);
        assert!(ns[idx(&spans, 1)].parent_id == -1); // root: Tempo nestedSetParent sentinel
        let root = ns[idx(&spans, 1)];
        let child = ns[idx(&spans, 4)];
        assert!(root.left < child.left && child.right < root.right);
        for entry in &ns {
            assert!(entry.left < entry.right);
        }
    }
}

mod assign_nested_set;
mod attr_prefix;
mod col_child_count;
mod col_duration;
mod col_event_name;
mod col_event_time_since_start;
mod col_instrumentation_name;
mod col_instrumentation_version;
mod col_kind;
mod col_link_span_id;
mod col_link_trace_id;
mod col_name;
mod col_ns_left;
mod col_ns_right;
mod col_parent_id;
mod col_parent_span_id;
mod col_root_service_name;
mod col_root_span_name;
mod col_span_id;
mod col_start;
mod col_status_code;
mod col_status_message;
mod col_trace_duration;
mod col_trace_id;
mod col_trace_start;
mod event_attr_prefix;
mod input_span;
mod instrumentation_attr_prefix;
mod link_attr_prefix;
mod nested_set;
mod span_schema;
mod span_schema_with_attrs;

pub use assign_nested_set::assign_nested_set;
pub use attr_prefix::ATTR_PREFIX;
pub use col_child_count::COL_CHILD_COUNT;
pub use col_duration::COL_DURATION;
pub use col_event_name::COL_EVENT_NAME;
pub use col_event_time_since_start::COL_EVENT_TIME_SINCE_START;
pub use col_instrumentation_name::COL_INSTRUMENTATION_NAME;
pub use col_instrumentation_version::COL_INSTRUMENTATION_VERSION;
pub use col_kind::COL_KIND;
pub use col_link_span_id::COL_LINK_SPAN_ID;
pub use col_link_trace_id::COL_LINK_TRACE_ID;
pub use col_name::COL_NAME;
pub use col_ns_left::COL_NS_LEFT;
pub use col_ns_right::COL_NS_RIGHT;
pub use col_parent_id::COL_PARENT_ID;
pub use col_parent_span_id::COL_PARENT_SPAN_ID;
pub use col_root_service_name::COL_ROOT_SERVICE_NAME;
pub use col_root_span_name::COL_ROOT_SPAN_NAME;
pub use col_span_id::COL_SPAN_ID;
pub use col_start::COL_START;
pub use col_status_code::COL_STATUS_CODE;
pub use col_status_message::COL_STATUS_MESSAGE;
pub use col_trace_duration::COL_TRACE_DURATION;
pub use col_trace_id::COL_TRACE_ID;
pub use col_trace_start::COL_TRACE_START;
pub use event_attr_prefix::EVENT_ATTR_PREFIX;
pub use input_span::InputSpan;
pub use instrumentation_attr_prefix::INSTRUMENTATION_ATTR_PREFIX;
pub use link_attr_prefix::LINK_ATTR_PREFIX;
pub use nested_set::NestedSet;
pub use span_schema::span_schema;
pub use span_schema_with_attrs::span_schema_with_attrs;
