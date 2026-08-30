//! Build span block `RecordBatch` values from internal spans.

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use krabka_blockstore::{
    AttrValue as BlockAttrValue, NestedSet as BlockNestedSet, PromotedSpanAttr, SpanAttr,
    SpanEvent, SpanKind, SpanLink, SpanRow, StatusCode, encode_span_rows_with_promoted_attrs,
};
use krabka_units::prelude::*;

use super::{AttrValue, KeyValue, Span, nested_set::assign_nested_set};
use crate::error::TracesError;

#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, BooleanArray, FixedSizeBinaryArray, Int32Array, ListArray, StringArray,
    };
    use assert2::check;
    use krabka_blockstore::{
        SCOL_ATTR_IS_ARRAY, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, SCOL_NESTED_SET_LEFT,
        SCOL_NESTED_SET_RIGHT, SCOL_PARENT_ID, SCOL_ROOT_SERVICE_NAME, SCOL_SPAN_ID, SCOL_TRACE_ID,
        span_block_schema,
    };

    use super::*;

    /// `extend_block_attr_value` appends one block attribute's values onto
    /// another of the same variant. The three non-string arms survived
    /// because nothing extended anything but strings -- and deleting an arm
    /// reaches an `unreachable!`, so the failure is a panic rather than a
    /// wrong answer. The order is pinned too: appending and prepending are
    /// both plausible and only differ when both sides are non-empty.
    #[test]
    fn extending_a_block_attribute_appends_to_its_own_variant() {
        let extend = |mut existing: BlockAttrValue, next: BlockAttrValue| {
            super::extend_block_attr_value(&mut existing, next);
            existing
        };

        check!(
            extend(
                BlockAttrValue::Str(vec!["a".into()]),
                BlockAttrValue::Str(vec!["b".into(), "c".into()]),
            ) == BlockAttrValue::Str(vec!["a".into(), "b".into(), "c".into()])
        );
        check!(
            extend(
                BlockAttrValue::Int(vec![1]),
                BlockAttrValue::Int(vec![2, 3])
            ) == BlockAttrValue::Int(vec![1, 2, 3])
        );
        check!(
            extend(
                BlockAttrValue::Double(vec![1.5]),
                BlockAttrValue::Double(vec![2.5, 3.5]),
            ) == BlockAttrValue::Double(vec![1.5, 2.5, 3.5])
        );
        check!(
            extend(
                BlockAttrValue::Bool(vec![true]),
                BlockAttrValue::Bool(vec![false, true]),
            ) == BlockAttrValue::Bool(vec![true, false, true])
        );
    }
    use crate::span::{
        EventRecord, KeyValue, LinkRecord, SpanKind as TraceKind, StatusCode as TraceStatus,
    };

    fn span(id: u8, parent: Option<u8>, root_svc: &str) -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: format!("s{id}"),
            kind: TraceKind::Server,
            start_ns: i64::from(id) * 10,
            duration_ns: 5,
            status: TraceStatus::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str(root_svc.into()),
            }],
            span_attrs: vec![KeyValue {
                key: "http.status_code".into(),
                value: AttrValue::Int(200),
            }],
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        }
    }

    fn col<'a, A: 'static>(batch: &'a RecordBatch, name: &str) -> &'a A {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<A>()
            .unwrap()
    }

    #[test]
    fn builds_batch_with_identity_and_nested_set() {
        let spans = vec![span(1, None, "api"), span(2, Some(1), "api")];
        let batch = span_batch(&spans).unwrap();
        assert2::assert!(batch.schema() == span_block_schema());
        assert2::assert!(batch.num_rows() == 2);

        let trace_ids = col::<FixedSizeBinaryArray>(&batch, SCOL_TRACE_ID);
        assert2::assert!(trace_ids.value(0) == [1; 16]);
        let span_ids = col::<FixedSizeBinaryArray>(&batch, SCOL_SPAN_ID);
        assert2::assert!(span_ids.value(0) == [1; 8]);

        let left = col::<Int32Array>(&batch, SCOL_NESTED_SET_LEFT);
        let right = col::<Int32Array>(&batch, SCOL_NESTED_SET_RIGHT);
        let parent_id = col::<Int32Array>(&batch, SCOL_PARENT_ID);
        assert2::assert!(left.values().as_ref() == &[1, 2]);
        assert2::assert!(right.values().as_ref() == &[4, 3]);
        // Root parent is -1 (Tempo nestedSetParent sentinel); the child's
        // parent_id equals the root's left value.
        assert2::assert!(parent_id.values().as_ref() == &[-1, 1]);

        let service = col::<StringArray>(&batch, SCOL_ROOT_SERVICE_NAME);
        assert2::assert!(service.value(0) == "api");
    }

    #[test]
    fn groups_repeated_attribute_keys_as_array_values() {
        let mut s = span(1, None, "api");
        s.span_attrs = vec![
            KeyValue {
                key: "http.method".into(),
                value: AttrValue::Str("GET".into()),
            },
            KeyValue {
                key: "http.method".into(),
                value: AttrValue::Str("POST".into()),
            },
        ];

        let batch = span_batch(&[s]).unwrap();

        let keys = col::<ListArray>(&batch, SCOL_ATTR_KEYS);
        let keys_row = keys.value(0);
        let keys = keys_row.as_any().downcast_ref::<StringArray>().unwrap();
        let methods_idx = (0..keys.len())
            .find(|idx| keys.value(*idx) == "http.method")
            .unwrap();
        assert2::assert!(
            (0..keys.len())
                .filter(|idx| keys.value(*idx) == "http.method")
                .count()
                == 1
        );

        let is_array = col::<ListArray>(&batch, SCOL_ATTR_IS_ARRAY);
        let is_array_row = is_array.value(0);
        let is_array = is_array_row
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert2::assert!(is_array.value(methods_idx));

        let values = col::<ListArray>(&batch, SCOL_ATTR_VALUE);
        let row_values = values.value(0);
        let row_values = row_values
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap_or_else(|| panic!("{SCOL_ATTR_VALUE} row is not a list"));
        let method_values = row_values.value(methods_idx);
        let method_values = method_values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert2::assert!(method_values.value(0) == "GET");
        assert2::assert!(method_values.value(1) == "POST");
    }

    fn attr_keys_of_row(batch: &RecordBatch, row: usize) -> Vec<String> {
        let keys = col::<ListArray>(batch, SCOL_ATTR_KEYS);
        let keys_row = keys.value(row);
        let keys = keys_row.as_any().downcast_ref::<StringArray>().unwrap();
        (0..keys.len())
            .map(|idx| keys.value(idx).to_string())
            .collect()
    }

    #[test]
    fn child_counts_match_tree_shape() {
        use krabka_blockstore::SCOL_CHILD_COUNT;

        // span 1 is root with two children (2, 3); span 2 has one child (4).
        let spans = vec![
            span(1, None, "api"),
            span(2, Some(1), "api"),
            span(3, Some(1), "api"),
            span(4, Some(2), "api"),
        ];
        let batch = span_batch(&spans).unwrap();
        let counts = col::<Int32Array>(&batch, SCOL_CHILD_COUNT);
        // Rows are index-aligned with input order: root has children 2 and 3,
        // span 2 has child 4, spans 3 and 4 are leaves.
        assert2::assert!(counts.values().as_ref() == &[2, 1, 0, 0]);
    }

    #[test]
    fn span_attr_cannot_spoof_resource_scope() {
        let mut s = span(1, None, "api");
        // A real resource attr legitimately gets the `__resource.` prefix downstream.
        s.resource_attrs.push(KeyValue {
            key: "deployment.environment".into(),
            value: AttrValue::Str("prod".into()),
        });
        // A client span attr keyed to look like a resource attr must NOT be encoded
        // into the resource namespace (TraceQL `resource.` scope bypass / spoof).
        s.span_attrs.push(KeyValue {
            key: format!("{RESOURCE_ATTR_PREFIX}service.name"),
            value: AttrValue::Str("evil".into()),
        });

        let batch = span_batch(&[s]).unwrap();
        let keys = attr_keys_of_row(&batch, 0);

        // The real resource attr is present under the resource namespace.
        assert2::assert!(keys.contains(&format!("{RESOURCE_ATTR_PREFIX}deployment.environment")));
        // The legitimate resource service.name is present exactly once.
        let resource_service_key = format!("{RESOURCE_ATTR_PREFIX}service.name");
        assert2::assert!(
            keys.iter()
                .filter(|key| **key == resource_service_key)
                .count()
                == 1
        );
        // The spoofed span attr (whose value was "evil") did NOT land in the
        // resource namespace: there is no second `__resource.service.name` entry,
        // and the resource value remains the true "api".
        let values = col::<ListArray>(&batch, SCOL_ATTR_VALUE);
        let row_values = values.value(0);
        let row_values = row_values.as_any().downcast_ref::<ListArray>().unwrap();
        let resource_service_idx = keys
            .iter()
            .position(|key| *key == resource_service_key)
            .unwrap();
        let service_values = row_values.value(resource_service_idx);
        let service_values = service_values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert2::assert!(service_values.len() == 1);
        assert2::assert!(service_values.value(0) == "api");
    }

    #[test]
    fn carries_events_and_links_through_schema() {
        let mut s = span(1, None, "api");
        s.events.push(EventRecord {
            time_unix_nano: 15,
            name: "exception".into(),
            attrs: vec![KeyValue {
                key: "exception.type".into(),
                value: AttrValue::Str("IO".into()),
            }],
        });
        s.links.push(LinkRecord {
            trace_id: [9; 16],
            span_id: [8; 8],
            attrs: Vec::new(),
        });

        let batch = span_batch(&[s]).unwrap();
        assert2::assert!(batch.num_rows() == 1);
        assert2::assert!(
            batch
                .column_by_name(krabka_blockstore::SCOL_EVENTS)
                .unwrap()
                .len()
                == 1
        );
        assert2::assert!(
            batch
                .column_by_name(krabka_blockstore::SCOL_LINKS)
                .unwrap()
                .len()
                == 1
        );
    }
}

// === split-modules: generated submodules ===
mod block_attr_value;
mod block_kind;
mod block_status;
mod child_counts;
mod event_attr_value;
mod event_attrs;
mod extend_block_attr_value;
mod push_span_attr;
mod resource_attr_prefix;
mod root_info;
mod same_block_attr_type;
mod service_name;
mod span_attrs;
mod span_batch;
mod span_batch_for_window;
mod span_batch_with_promoted_attrs;
mod span_events;
mod span_links;

use block_attr_value::block_attr_value;
use block_kind::block_kind;
use block_status::block_status;
use child_counts::child_counts;
use event_attr_value::event_attr_value;
use event_attrs::event_attrs;
use extend_block_attr_value::extend_block_attr_value;
use push_span_attr::push_span_attr;
pub use resource_attr_prefix::RESOURCE_ATTR_PREFIX;
use root_info::root_info;
use same_block_attr_type::same_block_attr_type;
use service_name::service_name;
use span_attrs::span_attrs;
pub use span_batch::span_batch;
pub use span_batch_for_window::span_batch_for_window;
pub use span_batch_with_promoted_attrs::span_batch_with_promoted_attrs;
use span_events::span_events;
use span_links::span_links;
