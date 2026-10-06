use std::{
    fmt::{self, Write},
    sync::Arc,
};

use krabka_domain_macros::TypeNameDisplay;

#[derive(TypeNameDisplay)]
struct Unit;

#[derive(TypeNameDisplay)]
struct Tuple(NotDisplay);

#[derive(TypeNameDisplay)]
struct Named {
    value: NotDisplay,
}

struct NotDisplay;

#[derive(TypeNameDisplay)]
struct Generic<'a, T: ?Sized, const N: usize = 3>
where
    T: 'a,
{
    value: &'a T,
    bytes: [u8; N],
}

#[derive(TypeNameDisplay)]
struct Defaulted<T = NotDisplay>(T);

#[derive(TypeNameDisplay)]
struct Callback(fn() -> ::core::primitive::usize);

#[derive(TypeNameDisplay)]
struct QualifiedTuple(object_store::memory::InMemory);

#[derive(TypeNameDisplay)]
struct QualifiedNamed {
    store: Arc<object_store::memory::InMemory>,
    active_gets: Arc<std::sync::atomic::AtomicUsize>,
    ranges: std::sync::Mutex<Vec<std::ops::Range<u64>>>,
}

#[derive(TypeNameDisplay)]
struct TupleWhere<T>(T)
where
    T: Fn() -> usize;

#[derive(TypeNameDisplay)]
struct QualifiedWhere<T>(T)
where
    T: std::fmt::Debug;

#[derive(TypeNameDisplay)]
struct r#Store;

struct RejectWrites;

impl Write for RejectWrites {
    fn write_str(&mut self, _: &str) -> fmt::Result {
        Err(fmt::Error)
    }
}

#[test]
fn struct_shapes_display_only_the_identifier() {
    assert2::assert!(Unit.to_string() == "Unit");
    assert2::assert!(Tuple(NotDisplay).to_string() == "Tuple");
    let named = Named { value: NotDisplay };
    assert2::assert!(named.to_string() == "Named");
    let Named { value } = named;
    let _: NotDisplay = value;
}

#[test]
fn generics_keep_bounds_defaults_and_unsized_fields_without_display_bounds() {
    let value = NotDisplay;
    let generic = Generic {
        value: &value,
        bytes: [1, 2, 3],
    };
    assert2::assert!(generic.to_string() == "Generic");
    let _: &NotDisplay = generic.value;
    assert2::assert!(generic.bytes == [1, 2, 3]);
    let slice = Generic {
        value: &[NotDisplay, NotDisplay][..],
        bytes: [4, 5],
    };
    assert2::assert!(slice.to_string() == "Generic");
    assert2::assert!(Defaulted(NotDisplay).to_string() == "Defaulted");
    let callback = Callback(|| 7);
    assert2::assert!(callback.to_string() == "Callback");
    assert2::assert!((callback.0)() == 7);
}

#[test]
fn raw_identifiers_display_without_the_raw_prefix() {
    assert2::assert!(r#Store.to_string() == "Store");
}

#[test]
fn qualified_field_paths_and_tuple_where_clauses_do_not_affect_display() {
    let tuple = QualifiedTuple(object_store::memory::InMemory::new());
    assert2::assert!(tuple.to_string() == "QualifiedTuple");
    let named = QualifiedNamed {
        store: Arc::new(tuple.0),
        active_gets: Arc::default(),
        ranges: std::sync::Mutex::default(),
    };
    assert2::assert!(named.to_string() == "QualifiedNamed");
    let QualifiedNamed {
        store,
        active_gets,
        ranges,
    } = named;
    assert2::assert!(Arc::strong_count(&store) == 1);
    assert2::assert!(active_gets.load(std::sync::atomic::Ordering::Relaxed) == 0);
    assert2::assert!(ranges.into_inner().unwrap().is_empty());
    let tuple_where = TupleWhere(|| 5);
    assert2::assert!(tuple_where.to_string() == "TupleWhere");
    assert2::assert!((tuple_where.0)() == 5);
    let qualified_where = QualifiedWhere(7);
    assert2::assert!(qualified_where.to_string() == "QualifiedWhere");
    assert2::assert!(qualified_where.0 == 7);
}

#[test]
fn formatting_preserves_write_str_behavior_and_propagates_writer_errors() {
    assert2::assert!(format!("{:>12.2}", Unit) == "Unit");
    assert2::assert!(write!(RejectWrites, "{Unit}").is_err());
}
