//! `remote_write` v2 string interning. `symbols[0]` is always the empty string.
//! All label names, label values, and metadata strings are `u32` indices into
//! `symbols`.

use std::collections::{HashMap, HashSet, hash_map::Entry};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// `symbols` hands back the table's contents in reference order, which is
    /// what the wire format indexes into. The order is the whole point: a
    /// table that returned the right strings in the wrong order would resolve
    /// every reference to the wrong symbol.
    #[test]
    fn the_symbol_list_is_returned_in_reference_order() {
        let mut table = SymbolTable::new();
        let first = table.intern("__name__");
        let second = table.intern("http_requests");
        let third = table.intern("job");

        // Index zero is the required empty symbol, and the rest follow in the
        // order they were interned.
        check!(
            table.symbols() == ["", "__name__", "http_requests", "job"],
            "got {:?}",
            table.symbols()
        );

        // The references the table handed out index into exactly that list.
        check!(table.symbols()[first as usize] == "__name__");
        check!(table.symbols()[second as usize] == "http_requests");
        check!(table.symbols()[third as usize] == "job");

        // A fresh table is not empty: it carries the zero symbol alone.
        check!(SymbolTable::new().symbols() == [""]);
    }

    #[test]
    fn intern_is_stable_and_zero_is_empty() {
        let mut t = SymbolTable::new();
        assert!(t.resolve(0) == Some(""));
        let a = t.intern("app");
        let b = t.intern("api");
        check!(t.intern("app") == a);
        check!(t.resolve(a) == Some("app"));
        check!(t.resolve(b) == Some("api"));
    }

    #[test]
    fn resolve_label_refs_pairs_names_and_values() {
        let mut t = SymbolTable::new();
        let app = t.intern("app");
        let api = t.intern("api");
        let env = t.intern("env");
        let prod = t.intern("prod");
        let labels = t.resolve_label_refs(&[app, api, env, prod]).unwrap();
        assert!(labels == vec![("app".into(), "api".into()), ("env".into(), "prod".into())]);
    }

    #[test]
    fn odd_length_refs_rejected() {
        let t = SymbolTable::new();
        assert!(t.resolve_label_refs(&[1]).is_err());
    }

    #[test]
    fn from_symbols_requires_empty_first() {
        assert!(SymbolTable::from_symbols(vec!["x".into()]).is_err());
        assert!(SymbolTable::from_symbols(vec![String::new(), "x".into()]).is_ok());
    }

    #[test]
    fn from_symbols_rejects_duplicates() {
        assert!(SymbolTable::from_symbols(vec![String::new(), "x".into(), "x".into()]).is_err());
    }

    #[test]
    fn resolve_label_refs_rejects_out_of_range_refs() {
        let t = SymbolTable::new();
        assert!(t.resolve_label_refs(&[0, 7]).is_err());
    }

    #[test]
    fn resolve_label_refs_rejects_duplicate_label_names() {
        let mut t = SymbolTable::new();
        let job = t.intern("job");
        let api = t.intern("api");
        let worker = t.intern("worker");

        let err = t.resolve_label_refs(&[job, api, job, worker]).unwrap_err();

        assert!(matches!(err, SymbolError::DuplicateLabel(name) if name == "job"));
    }
}

mod symbol_error;
mod symbol_table;

pub use symbol_error::SymbolError;
pub use symbol_table::SymbolTable;
