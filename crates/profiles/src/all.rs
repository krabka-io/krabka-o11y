//! What a single-process profiles stack is made of, and the order it stops in.
//!
//! `krabka-profiles --target all` runs every profiles role in one process.
//! The composition itself lives in the binary, but the stop order does not:
//! it is a property of the signal's pipeline rather than of one `main`, and a
//! test that cannot see the binary's private modules still has to be able to
//! pin it.

use krabka_observability::RoleKind;

mod drain_order;

pub use drain_order::DRAIN_ORDER;
