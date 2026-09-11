//! What a control says about a filter it cannot move.
//!
//! The suite under `tests/log_level.rs` installs a reloadable subscriber and
//! drives `/log_level` through it, which covers the path where the filter
//! does move. A subscriber can only be installed once per process, so that
//! suite cannot also be the process that installed none -- and that is the
//! process this module's doc comment is about: one that reports `success` and
//! changes nothing sends an operator looking for the bug somewhere else.

use assert2::{assert, check};

use super::{EnvFilter, LogLevelControl, LogLevelError};

#[test]
fn a_fixed_control_reports_its_level_and_refuses_to_move_it() {
    let control = LogLevelControl::fixed("warn");

    check!(control.level() == "warn");
    check!(
        !control.is_reloadable(),
        "a control with no reload handle says so, so a route can answer 501 rather than 200"
    );

    let refused = control.set_level("debug");

    assert!(let Err(LogLevelError::Fixed) = &refused);
    check!(
        control.level() == "warn",
        "a change that was refused does not move the level the control reports"
    );
    check!(
        refused
            .expect_err("a fixed control refuses every change")
            .to_string()
            .contains("RUST_LOG"),
        "the refusal names the variable that does set this process's level"
    );
}

/// A clone is the same control, not a copy of its level. The HTTP route holds
/// one clone and start-up kept another, so a change through either has to be
/// visible through both.
#[test]
fn a_clone_of_a_control_names_the_same_filter() {
    let control = LogLevelControl::fixed("info");
    let clone = control.clone();

    check!(clone.level() == "info");
    check!(clone.is_reloadable() == control.is_reloadable());
    assert!(let Err(LogLevelError::Fixed) = clone.set_level("trace"));
}

/// `/log_level` reports one word, and `RUST_LOG` is a list of directives. The
/// word is the most verbose thing the filter admits anywhere, because that is
/// the question an operator who set `debug` on one target is asking.
#[test]
fn the_reported_word_is_the_most_verbose_level_the_filter_admits() {
    let cases = [
        ("info", "info"),
        ("debug", "debug"),
        ("warn", "warn"),
        // A per-target directive is more verbose than the global one, and it
        // is the reason lines are being emitted at all.
        ("info,krabka_observability=debug", "debug"),
        // And less verbose: the global level still governs everything else.
        ("info,hyper=off", "info"),
        // A filter that admits nothing says so rather than defaulting.
        ("off", "off"),
    ];

    for (directives, want) in cases {
        check!(
            LogLevelControl::level_word(&EnvFilter::new(directives)) == want,
            "RUST_LOG={directives}"
        );
    }
}
