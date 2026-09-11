//! What a config file is refused for, and how its values reach `clap`.
//!
//! The suite under `tests/config_file.rs` drives the whole helper through a
//! real binary's `Cli`, which is the right shape for the precedence rule. It
//! cannot reach most of the refusals: a `Cli` has no positional argument and
//! no counted switch, and a refusal it does reach arrives as one
//! `ConfigFileError` among several with the same `Display`. These cases go at
//! [`file_argument_overrides`] directly, one per way a file is wrong, because
//! a file that is half-applied is what this module exists to prevent.

use assert2::{assert, check};
use clap::Arg;

use super::{
    ArgAction, ArgMatches, Command, ConfigFileError, FsPath, OsString, YamlValue,
    file_argument_overrides, yaml_scalar,
};

/// A command with one argument of every shape `file_argument_overrides`
/// branches on: a plain value, a repeatable value, both switch actions, a
/// counted switch, and a positional the file must not be able to set.
fn command() -> Command {
    Command::new("config-file-unit")
        .arg(Arg::new("target").long("target"))
        .arg(
            Arg::new("tenant")
                .long("tenant")
                .action(ArgAction::Append)
                .num_args(1),
        )
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .action(ArgAction::SetTrue),
        )
        .arg(Arg::new("quiet").long("quiet").action(ArgAction::SetFalse))
        .arg(
            Arg::new("verbosity")
                .long("verbosity")
                .action(ArgAction::Count),
        )
        .arg(Arg::new("input").index(1))
}

/// The first, error-tolerant parse the helper does of the real command line.
fn matches(argv: &[&str]) -> ArgMatches {
    command()
        .ignore_errors(true)
        .try_get_matches_from(std::iter::once("config-file-unit").chain(argv.iter().copied()))
        .expect("a tolerant parse accepts this command line")
}

fn path() -> &'static FsPath {
    FsPath::new("/etc/krabka/krabka.yaml")
}

fn document(body: &str) -> YamlValue {
    serde_yaml::from_str(body).expect("the test document is YAML")
}

/// Runs the helper over `body` against a command line of `argv`.
fn overrides(body: &str, argv: &[&str]) -> Result<Vec<OsString>, ConfigFileError> {
    file_argument_overrides(&command(), &matches(argv), path(), &document(body))
}

/// The arguments the helper produced, as the strings a command line is made
/// of, so a case can state the whole expected argument list rather than
/// picking at it.
fn arguments(body: &str) -> Vec<String> {
    overrides(body, &[])
        .expect("this document sets arguments")
        .into_iter()
        .map(|argument| {
            argument
                .into_string()
                .expect("the helper builds UTF-8 arguments")
        })
        .collect()
}

#[test]
fn an_empty_file_sets_nothing_and_is_not_an_error() {
    // `serde_yaml` parses an empty document as `Null`, which is what a file
    // holding only comments parses to as well. A deployment that comments its
    // last key out has an empty file, not a broken one.
    check!(document("") == YamlValue::Null);
    check!(arguments("") == Vec::<String>::new());
    check!(arguments("# every key is commented out\n") == Vec::<String>::new());
}

#[test]
fn a_document_that_is_not_a_mapping_of_keys_to_values_is_refused() {
    for body in ["just a string", "- a\n- b\n", "3"] {
        let refused = overrides(body, &[]).expect_err("this is not a mapping");
        // The file it read is named, because a process reading several files
        // has to say which one it will not start on.
        let ConfigFileError::NotAMapping { path: named } = &refused else {
            panic!("{body:?} was refused as {refused:?}")
        };
        check!(named == path(), "{body:?} names the file it read");
        check!(refused.to_string().contains("krabka.yaml"), "{body:?}");
    }
}

#[test]
fn a_key_that_is_not_a_name_is_refused_rather_than_stringified() {
    let refused = overrides("3: value\n", &[]).expect_err("a number is not a flag name");
    assert!(let ConfigFileError::NonStringKey { .. } = &refused);
    // The key reaches the message, because an operator reading it has to find
    // the line in the file.
    check!(refused.to_string().contains('3'));
}

/// `--config.file` and `--config.expand-env` are read out of the command line
/// before the file is opened, so a file that set either would be asking to be
/// read differently than it was just read. Both are refused by name.
#[test]
fn the_file_cannot_set_the_two_flags_that_found_it() {
    for key in ["config.file", "config.expand-env"] {
        let refused = overrides(&format!("{key}: elsewhere.yaml\n"), &[])
            .expect_err("a self-referential key is refused");
        check!(
            let ConfigFileError::SelfReferentialKey { .. } = &refused,
            "{key} is refused as self-referential"
        );
        check!(refused.to_string().contains(key), "{key} names itself");
    }
}

#[test]
fn a_key_that_names_a_positional_argument_is_refused() {
    // `input` is a real argument of the command, so this is not an unknown
    // key. It has no long spelling, so there is no `--input=...` to emit and
    // the file cannot supply it at all.
    let refused = overrides("input: /var/lib/krabka\n", &[]).expect_err("a positional is refused");
    assert!(let ConfigFileError::PositionalKey { .. } = &refused);
    check!(refused.to_string().contains("input"));
}

#[test]
fn a_key_that_names_no_flag_of_this_binary_is_refused() {
    let refused = overrides("listen_addr: 0.0.0.0:3100\n", &[]).expect_err("no such flag");
    assert!(let ConfigFileError::UnknownKey { .. } = &refused);
    check!(refused.to_string().contains("listen_addr"));
}

/// Each action reads its value its own way, so each has its own refusal. A
/// switch given a string and a counter given a string are different mistakes
/// and say so.
#[test]
fn a_value_of_the_wrong_kind_names_the_kind_the_flag_needs() {
    let cases = [
        ("verbose: \"yes\"\n", "a boolean"),
        ("quiet: 1\n", "a boolean"),
        ("verbosity: \"loud\"\n", "a whole number"),
        ("verbosity: -1\n", "a whole number"),
        // A value the flag takes one of, and an item inside a sequence of
        // them: both reach the scalar renderer, and neither is a scalar.
        ("target: {a: b}\n", "a string, a number or a boolean"),
        ("tenant:\n  - [nested]\n", "a string, a number or a boolean"),
    ];
    for (body, wanted_kind) in cases {
        let refused = overrides(body, &[]).expect_err("this value is the wrong kind");
        check!(
            let ConfigFileError::UnsupportedValue { .. } = &refused,
            "{body:?} is refused as an unsupported value"
        );
        if let ConfigFileError::UnsupportedValue { wanted, .. } = &refused {
            check!(*wanted == wanted_kind, "{body:?}");
        }
        check!(refused.to_string().contains(wanted_kind), "{body:?}");
    }
}

/// A switch is present or absent on a command line, so the file states the
/// state it wants and the helper emits the flag only when the flag would
/// produce that state.
#[test]
fn a_switch_is_emitted_only_when_the_file_asks_for_the_state_the_flag_sets() {
    check!(arguments("verbose: true\n") == vec!["--verbose".to_string()]);
    check!(
        arguments("verbose: false\n") == Vec::<String>::new(),
        "`false` for a switch that sets true is the default, and sets nothing"
    );
    // `--quiet` sets its value *false*, so the file asking for `false` is the
    // file asking for the flag.
    check!(arguments("quiet: false\n") == vec!["--quiet".to_string()]);
    check!(arguments("quiet: true\n") == Vec::<String>::new());
}

#[test]
fn a_counted_switch_is_repeated_as_many_times_as_the_file_asks() {
    check!(arguments("verbosity: 0\n") == Vec::<String>::new());
    check!(arguments("verbosity: 1\n") == vec!["--verbosity".to_string()]);
    check!(
        arguments("verbosity: 3\n")
            == vec![
                "--verbosity".to_string(),
                "--verbosity".to_string(),
                "--verbosity".to_string(),
            ]
    );
}

#[test]
fn a_sequence_becomes_one_flag_per_item_and_a_scalar_becomes_one() {
    check!(
        arguments("tenant:\n  - alpha\n  - beta\n")
            == vec!["--tenant=alpha".to_string(), "--tenant=beta".to_string()]
    );
    check!(arguments("target: blocks\n") == vec!["--target=blocks".to_string()]);
    // A scalar goes through the same renderer the command line would have
    // used, so `32` and `true` reach the flag's own value parser as the text
    // an operator would have typed.
    check!(arguments("target: 32\n") == vec!["--target=32".to_string()]);
    check!(arguments("target: true\n") == vec!["--target=true".to_string()]);
}

/// The precedence rule, at the one boundary this helper owns: it is handed the
/// first parse of the real command line and must supply nothing for an
/// argument that parse already found.
#[test]
fn an_argument_the_command_line_already_set_is_left_alone() {
    let body = "target: from-the-file\nverbosity: 2\n";
    check!(
        overrides(body, &["--target", "from-the-command-line"]).expect("the file parses")
            == vec![OsString::from("--verbosity"), OsString::from("--verbosity"),],
        "the file still supplies the arguments the command line did not"
    );
}

#[test]
fn a_scalar_is_rendered_the_way_the_command_line_would_have_spelled_it() {
    let rendered = |value: &str| {
        yaml_scalar(path(), "target", &document(value)).expect("this value is a scalar")
    };

    check!(rendered("0.0.0.0:3100") == "0.0.0.0:3100");
    check!(rendered("\"1m\"") == "1m");
    check!(rendered("32") == "32");
    check!(rendered("1.5") == "1.5");
    check!(rendered("true") == "true");
    check!(rendered("false") == "false");

    let refused =
        yaml_scalar(path(), "target", &document("{a: b}")).expect_err("a mapping is not a scalar");
    assert!(let ConfigFileError::UnsupportedValue { .. } = &refused);
    check!(refused.to_string().contains("target"));
}
