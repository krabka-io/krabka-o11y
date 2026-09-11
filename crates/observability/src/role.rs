//! The role vocabulary, shared by every signal.
//!
//! Each signal used to name its own write path. The stage between the
//! distributor and object storage was a `Compactor` in metrics and logs and a
//! `BlockBuilder` in traces and profiles -- where `Compactor` was a second,
//! different role that merged blocks already in the store. One word named two
//! jobs depending on which binary an operator was reading, which is a hazard
//! in a runbook and in a chart.
//!
//! [`RoleKind`] is the settled vocabulary. It is not a binary's `--target`
//! enum: no signal runs every stage, and a binary's own enum is what makes
//! `--help` honest about the roles that binary has. It is the one place the
//! *names* are written down, so a per-signal enum spells a stage the way every
//! other signal spells it, and a test can hold that.

mod role_kind;
#[cfg(test)]
mod tests;

pub use role_kind::RoleKind;
