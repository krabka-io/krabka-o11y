//! The invalid values a signal binary's WAL fetch limits must refuse.
//!
//! `--wal-fetch-max` and `--wal-fetch-partition-max` become Kafka fetch sizes,
//! which are positive `i32` byte counts. The `krabka-traces` and
//! `krabka-profiles` binaries parse both flags the same way and reach this
//! file with `#[path]`, so it depends only on `clap` and `assert2`.

use clap::Parser;

/// Values neither WAL fetch limit accepts: zero, non-numbers, negative and
/// fractional byte counts, and one byte past `i32::MAX`.
const INVALID_WAL_FETCH_LIMITS: &[&str] = &["0", "not-a-number", "-1B", "1.5B", "2147483648B"];

/// Asserts that `binary`'s CLI, parsed as `Cli`, refuses every invalid value of
/// either WAL fetch limit on a `--target block-builder` command line.
pub fn assert_rejects_invalid_wal_fetch_limits<Cli: Parser>(binary: &str) {
    for flag in ["--wal-fetch-max", "--wal-fetch-partition-max"] {
        for invalid in INVALID_WAL_FETCH_LIMITS {
            assert2::assert!(
                Cli::try_parse_from([binary, "--target", "block-builder", flag, invalid]).is_err(),
                "{flag} should reject {invalid:?}"
            );
        }
    }
}
