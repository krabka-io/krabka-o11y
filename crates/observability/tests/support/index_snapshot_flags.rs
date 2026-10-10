//! The invalid values a signal binary's index snapshot policy must refuse.
//!
//! `--index-snapshot-max` and `--index-snapshot-retain` are positive counts,
//! and `--index-snapshot-max` also takes a whole byte size. The
//! `krabka-traces` and `krabka-profiles` binaries parse both flags the same
//! way and reach this file with `#[path]`, so it depends only on `clap` and
//! `assert2`.

use clap::Parser;

/// Values neither flag accepts as a count: zero, non-numbers, negative
/// numbers, and one past `u64::MAX`.
const INVALID_SNAPSHOT_COUNTS: &[&str] = &["0", "not-a-number", "-1", "18446744073709551616"];

/// Byte sizes `--index-snapshot-max` refuses: a fractional byte count and one
/// past `u64::MAX` bytes.
const INVALID_SNAPSHOT_MAX_SIZES: &[&str] = &["1.5B", "18446744073709551616B"];

/// Asserts that `binary`'s CLI, parsed as `Cli`, refuses every invalid index
/// snapshot policy value on a `--target block-builder` command line.
pub fn assert_rejects_invalid_index_snapshot_policy<Cli: Parser>(binary: &str) {
    for flag in ["--index-snapshot-max", "--index-snapshot-retain"] {
        for invalid in INVALID_SNAPSHOT_COUNTS {
            assert2::assert!(
                Cli::try_parse_from([binary, "--target", "block-builder", flag, invalid]).is_err(),
                "{flag} should reject {invalid:?}"
            );
        }
    }
    for invalid in INVALID_SNAPSHOT_MAX_SIZES {
        assert2::assert!(
            Cli::try_parse_from([
                binary,
                "--target",
                "block-builder",
                "--index-snapshot-max",
                invalid,
            ])
            .is_err(),
            "--index-snapshot-max should reject {invalid:?}"
        );
    }
}
