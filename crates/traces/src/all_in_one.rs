//! What `--target all` composes, and the order it takes apart.
//!
//! The composition itself lives in the binary, because it is made of the same
//! `run_*` functions a single-role process uses. What lives here is the one
//! part of it that has to be checkable without starting seven roles and a
//! broker: the order the roles stop in. It is a value rather than a sequence
//! of calls so that the process and its tests read the same answer, and so
//! that adding a role to the composition is a change to this list rather than
//! a line inserted somewhere in the middle of a start-up function.

use krabka_observability::RoleKind;

mod drain_order;

pub use drain_order::DRAIN_ORDER;
