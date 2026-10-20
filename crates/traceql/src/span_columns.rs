//! `TraceQL` span column names and structural interval helpers.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use krabka_blockstore::{CycleSpans, SpanNode};
use krabka_units::Time;

use crate::result::{EventRef, LinkRef};

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::{assert, check};

    use super::*;
    use crate::testkit::span::SpanFixture;

    struct SchemaColumn {
        name: &'static str,
        data_type: DataType,
    }

    fn sid(n: u8) -> [u8; 8] {
        [n; 8]
    }

    fn span(id: u8, parent: Option<u8>) -> InputSpan {
        SpanFixture {
            trace: 7,
            id,
            parent,
            ..SpanFixture::default()
        }
        .input_span()
    }

    fn four_spans() -> Vec<InputSpan> {
        vec![
            span(1, None),
            span(2, Some(1)),
            span(3, Some(1)),
            span(4, Some(3)),
        ]
    }

    fn idx(spans: &[InputSpan], id: u8) -> usize {
        spans.iter().position(|s| s.span_id == sid(id)).unwrap()
    }

    #[test]
    fn schema_contains_traceql_planning_columns() {
        let schema = span_schema();
        for column in [
            SchemaColumn {
                name: COL_TRACE_ID,
                data_type: DataType::FixedSizeBinary(16),
            },
            SchemaColumn {
                name: COL_NAME,
                data_type: DataType::Utf8,
            },
            SchemaColumn {
                name: COL_NS_LEFT,
                data_type: DataType::Int32,
            },
            SchemaColumn {
                name: COL_CHILD_COUNT,
                data_type: DataType::Int32,
            },
            SchemaColumn {
                name: COL_TRACE_DURATION,
                data_type: DataType::Int64,
            },
        ] {
            assert!(
                schema.column_with_name(column.name).unwrap().1.data_type() == &column.data_type,
                "column: {}",
                column.name
            );
        }
    }

    #[test]
    fn attr_prefix_matches_tempo_virtual_attribute_shape() {
        assert!(ATTR_PREFIX == "attr.");
    }

    #[test]
    fn nested_set_parent_id_is_parent_left() {
        let spans = four_spans();
        let ns = assign_nested_set(&spans);
        let root_left = ns[idx(&spans, 1)].left;
        check!(ns[idx(&spans, 1)].parent_id == -1); // root: Tempo nestedSetParent sentinel
        check!(ns[idx(&spans, 2)].parent_id == root_left);
        check!(ns[idx(&spans, 3)].parent_id == root_left);
        check!(ns[idx(&spans, 4)].parent_id == ns[idx(&spans, 3)].left);
    }

    #[test]
    fn nested_set_intervals_identify_ancestors() {
        let spans = four_spans();
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

mod input_span;

pub use input_span::InputSpan;

pub const ATTR_PREFIX: &str = "attr.";

pub const COL_CHILD_COUNT: &str = "child_count";

pub const COL_DURATION: &str = "duration_nanos";

pub const COL_EVENT_NAME: &str = "event_name";

pub const COL_EVENT_TIME_SINCE_START: &str = "event_time_since_start_nanos";

pub const COL_INSTRUMENTATION_NAME: &str = "instrumentation_name";

pub const COL_INSTRUMENTATION_VERSION: &str = "instrumentation_version";

pub const COL_KIND: &str = "kind";

pub const COL_LINK_SPAN_ID: &str = "link_span_id";

pub const COL_LINK_TRACE_ID: &str = "link_trace_id";

pub const COL_NAME: &str = "name";

pub const COL_NS_LEFT: &str = "nested_set_left";

pub const COL_NS_RIGHT: &str = "nested_set_right";

pub const COL_PARENT_ID: &str = "parent_id";

pub const COL_PARENT_SPAN_ID: &str = "parent_span_id";

pub const COL_ROOT_SERVICE_NAME: &str = "root_service_name";

pub const COL_ROOT_SPAN_NAME: &str = "root_span_name";

pub const COL_SPAN_ID: &str = "span_id";

pub const COL_START: &str = "start_unix_nano";

pub const COL_STATUS_CODE: &str = "status_code";

pub const COL_STATUS_MESSAGE: &str = "status_message";

pub const COL_TRACE_DURATION: &str = "trace_duration_nanos";

pub const COL_TRACE_ID: &str = "trace_id";

pub const COL_TRACE_START: &str = "trace_start_unix_nano";

pub const EVENT_ATTR_PREFIX: &str = "__event.";

pub const INSTRUMENTATION_ATTR_PREFIX: &str = "__instrumentation.";

pub const LINK_ATTR_PREFIX: &str = "__link.";

#[must_use]
pub fn span_schema() -> SchemaRef {
    span_schema_with_attrs(&[])
}

/// One span's nested-set assignment, aligned by index with the input spans.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NestedSet {
    pub left: i32,
    pub right: i32,
    pub parent_id: i32,
}

impl From<krabka_blockstore::NestedSet> for NestedSet {
    fn from(block: krabka_blockstore::NestedSet) -> Self {
        Self {
            left: block.nested_set_left,
            right: block.nested_set_right,
            parent_id: block.parent_id,
        }
    }
}

/// Assigns nested-set intervals to `spans`, aligned by index.
///
/// Delegates to [`krabka_blockstore::assign_nested_set`], which owns the
/// traversal, the Tempo `-1` root sentinel, and the cycle handling.
#[must_use]
pub fn assign_nested_set(spans: &[InputSpan]) -> Vec<NestedSet> {
    let nodes: Vec<SpanNode> = spans
        .iter()
        .map(|span| SpanNode {
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
        })
        .collect();
    krabka_blockstore::assign_nested_set(&nodes, CycleSpans::AssignIntervals)
        .into_iter()
        .map(NestedSet::from)
        .collect()
}

#[derive(Clone, Debug)]
pub struct AttributeColumn {
    pub key: String,
    pub data_type: DataType,
}

#[must_use]
pub fn span_schema_with_attrs(attr_cols: &[AttributeColumn]) -> SchemaRef {
    let mut fields = vec![
        Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
        Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
        Field::new(COL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(COL_NS_LEFT, DataType::Int32, false),
        Field::new(COL_NS_RIGHT, DataType::Int32, false),
        Field::new(COL_PARENT_ID, DataType::Int32, false),
        Field::new(COL_CHILD_COUNT, DataType::Int32, false),
        Field::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, true),
        Field::new(COL_ROOT_SPAN_NAME, DataType::Utf8, true),
        Field::new(COL_TRACE_START, DataType::Int64, false),
        Field::new(COL_TRACE_DURATION, DataType::Int64, false),
        Field::new(COL_NAME, DataType::Utf8, true),
        Field::new(COL_KIND, DataType::Int32, false),
        Field::new(COL_START, DataType::Int64, false),
        Field::new(COL_DURATION, DataType::Int64, false),
        Field::new(COL_STATUS_CODE, DataType::Int32, false),
        Field::new(COL_STATUS_MESSAGE, DataType::Utf8, true),
        Field::new(COL_INSTRUMENTATION_NAME, DataType::Utf8, true),
        Field::new(COL_INSTRUMENTATION_VERSION, DataType::Utf8, true),
        Field::new(COL_EVENT_NAME, DataType::Utf8, true),
        Field::new(COL_EVENT_TIME_SINCE_START, DataType::Int64, true),
        Field::new(COL_LINK_TRACE_ID, DataType::FixedSizeBinary(16), true),
        Field::new(COL_LINK_SPAN_ID, DataType::FixedSizeBinary(8), true),
    ];

    fields.extend(attr_cols.iter().map(|column| {
        Field::new(
            format!("{ATTR_PREFIX}{}", column.key),
            column.data_type.clone(),
            true,
        )
    }));

    Arc::new(Schema::new(fields))
}
