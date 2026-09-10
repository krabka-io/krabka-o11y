//! The LogQL parser, on arbitrary text.
//!
//! Both entry points are driven: `parse_query` for a stream selector and
//! `parse_logql_expr` for the recursive expression form, which is the one that
//! can nest and so the one where a deeply nested query is a cost the parser
//! has to bound.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|query: &str| {
    let _ = krabka_logql::parse_query(query);
    let _ = krabka_logql::parse_logql_expr(query);
});
