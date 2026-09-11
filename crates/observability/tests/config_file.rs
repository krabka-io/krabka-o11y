//! What a `--config.file` sets, and what outranks it.
//!
//! The precedence the helper promises is command line > environment > file >
//! default. Each case below sits on one of those boundaries, so a change that
//! reordered them could not pass by only losing a case at the top or bottom.

use std::{
    ffi::OsString,
    net::SocketAddr,
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
};

use assert2::{assert, check};
use clap::Parser;
use krabka_observability::{ConfigFileArgs, argv_with_config_file};

/// A binary's `Cli` in miniature: one required argument with no default, one
/// with a default, a switch, and a repeatable value.
#[derive(Debug, Parser, PartialEq, Eq)]
#[command(name = "config-file-test")]
struct TestCli {
    #[command(flatten)]
    config_file: ConfigFileArgs,
    #[arg(long, env = "KRABKA_TEST_CONFIG_TARGET")]
    target: String,
    #[arg(
        long,
        env = "KRABKA_TEST_CONFIG_LISTEN",
        default_value = "0.0.0.0:3100"
    )]
    listen: SocketAddr,
    #[arg(long, env = "KRABKA_TEST_CONFIG_VERBOSE")]
    verbose: bool,
    #[arg(long = "tenant")]
    tenants: Vec<String>,
    #[arg(long, env = "KRABKA_TEST_CONFIG_MAX_BODY")]
    max_body: Option<String>,
}

/// Every case here reads the same environment through `clap`, and some of them
/// set it. A `getenv` running while another thread is inside `setenv` is a race
/// in libc whatever the Rust-side locking says, so the whole file takes one
/// turn at a time rather than trusting the cases that only read.
static ENVIRONMENT: Mutex<()> = Mutex::new(());

fn lock_environment() -> MutexGuard<'static, ()> {
    // A case that fails poisons the lock; the ones after it are still worth
    // running, and they are no less isolated for it.
    ENVIRONMENT.lock().unwrap_or_else(PoisonError::into_inner)
}

fn write_config(directory: &Path, body: &str) -> String {
    let path = directory.join("krabka.yaml");
    std::fs::write(&path, body).expect("write config file");
    path.to_str().expect("utf-8 temp path").to_owned()
}

fn parse_with(config: &str, extra: &[&str]) -> TestCli {
    let directory = tempfile::tempdir().expect("temp dir");
    let path = write_config(directory.path(), config);
    let mut argv: Vec<OsString> = vec![
        "config-file-test".into(),
        "--config.file".into(),
        path.into(),
    ];
    argv.extend(extra.iter().map(OsString::from));
    let argv = argv_with_config_file::<TestCli>(argv).expect("config file applies");
    TestCli::parse_from(argv)
}

#[test]
fn a_config_file_supplies_every_kind_of_argument() {
    let _environment = lock_environment();
    let cli = parse_with(
        "target: querier\nlisten: 127.0.0.1:9999\nverbose: true\ntenant:\n  - one\n  - two\n",
        &[],
    );

    check!(cli.target == "querier");
    check!(cli.listen == "127.0.0.1:9999".parse::<SocketAddr>().expect("addr"));
    check!(cli.verbose);
    check!(cli.tenants == vec!["one".to_owned(), "two".to_owned()]);
}

#[test]
fn a_config_file_key_may_be_spelled_with_underscores_or_dashes() {
    let _environment = lock_environment();
    // The keys are the binary's long flags. A YAML habit of snake_case has to
    // reach the same flag a runbook spells with dashes, or half the file is
    // silently the default.
    let dashes = parse_with("target: querier\nmax-body: 4MiB\n", &[]);
    let underscores = parse_with("target: querier\nmax_body: 4MiB\n", &[]);

    check!(dashes.max_body == Some("4MiB".to_owned()));
    check!(underscores.max_body == dashes.max_body);
}

#[test]
fn precedence_runs_command_line_over_environment_over_file_over_default() {
    let _environment = lock_environment();
    // One config file, one environment, one command line, and four arguments
    // that differ only in how far up that list each one is set.
    let directory = tempfile::tempdir().expect("temp dir");
    let path = write_config(
        directory.path(),
        "target: from-file\nlisten: 10.0.0.1:1111\n",
    );

    temp_env::with_vars(
        [
            ("KRABKA_TEST_CONFIG_TARGET", Some("from-environment")),
            ("KRABKA_TEST_CONFIG_LISTEN", None::<&str>),
            ("KRABKA_TEST_CONFIG_VERBOSE", None),
            ("KRABKA_CONFIG_FILE", None),
            ("KRABKA_CONFIG_EXPAND_ENV", None),
        ],
        || {
            let argv: Vec<OsString> = vec![
                "config-file-test".into(),
                "--config.file".into(),
                path.clone().into(),
            ];
            let argv = argv_with_config_file::<TestCli>(argv).expect("config file applies");
            let cli = TestCli::parse_from(argv);

            // Environment over file.
            check!(cli.target == "from-environment");
            // File over default.
            check!(cli.listen == "10.0.0.1:1111".parse::<SocketAddr>().expect("addr"));
            // Default, with nothing above it.
            check!(!cli.verbose);

            let argv: Vec<OsString> = vec![
                "config-file-test".into(),
                "--config.file".into(),
                path.clone().into(),
                "--target".into(),
                "from-command-line".into(),
                "--listen".into(),
                "10.0.0.2:2222".into(),
            ];
            let argv = argv_with_config_file::<TestCli>(argv).expect("config file applies");
            let cli = TestCli::parse_from(argv);

            // Command line over environment.
            check!(cli.target == "from-command-line");
            // Command line over file.
            check!(cli.listen == "10.0.0.2:2222".parse::<SocketAddr>().expect("addr"));
        },
    );
}

#[test]
fn the_config_file_path_itself_comes_from_a_flag_or_the_environment() {
    let _environment = lock_environment();
    let directory = tempfile::tempdir().expect("temp dir");
    let path = write_config(directory.path(), "target: from-file\n");

    temp_env::with_vars(
        [
            ("KRABKA_CONFIG_FILE", Some(path.as_str())),
            ("KRABKA_TEST_CONFIG_TARGET", None),
        ],
        || {
            let argv = argv_with_config_file::<TestCli>(vec!["config-file-test".into()])
                .expect("config file applies");
            check!(TestCli::parse_from(argv).target == "from-file");
        },
    );
}

#[test]
fn a_key_that_names_no_flag_stops_start_up() {
    let _environment = lock_environment();
    let directory = tempfile::tempdir().expect("temp dir");
    let path = write_config(directory.path(), "target: querier\nlissten: 0.0.0.0:1\n");
    let argv: Vec<OsString> = vec![
        "config-file-test".into(),
        "--config.file".into(),
        path.into(),
    ];

    assert!(let Err(error) = argv_with_config_file::<TestCli>(argv));
    check!(error.to_string().contains("lissten"));
}

#[test]
fn a_missing_config_file_stops_start_up() {
    let _environment = lock_environment();
    let argv: Vec<OsString> = vec![
        "config-file-test".into(),
        "--config.file".into(),
        "/nonexistent/krabka.yaml".into(),
    ];

    assert!(let Err(error) = argv_with_config_file::<TestCli>(argv));
    check!(error.to_string().contains("/nonexistent/krabka.yaml"));
}

#[test]
fn expand_env_substitutes_only_when_asked() {
    let _environment = lock_environment();
    let directory = tempfile::tempdir().expect("temp dir");
    let path = write_config(
        directory.path(),
        "target: ${KRABKA_TEST_CONFIG_SECRET}\nlisten: ${KRABKA_TEST_CONFIG_ADDR:0.0.0.0:7777}\n",
    );

    temp_env::with_vars(
        [
            ("KRABKA_TEST_CONFIG_SECRET", Some("expanded")),
            ("KRABKA_TEST_CONFIG_ADDR", None),
            ("KRABKA_TEST_CONFIG_TARGET", None),
            ("KRABKA_TEST_CONFIG_LISTEN", None),
            ("KRABKA_CONFIG_FILE", None),
            ("KRABKA_CONFIG_EXPAND_ENV", None),
        ],
        || {
            let expanded: Vec<OsString> = vec![
                "config-file-test".into(),
                "--config.expand-env".into(),
                "--config.file".into(),
                path.clone().into(),
            ];
            let argv = argv_with_config_file::<TestCli>(expanded).expect("expansion applies");
            let cli = TestCli::parse_from(argv);
            check!(cli.target == "expanded");
            check!(cli.listen == "0.0.0.0:7777".parse::<SocketAddr>().expect("addr"));

            // Without the switch the reference is the literal value, which is
            // not a socket address -- so the file is taken at its word.
            let literal: Vec<OsString> = vec![
                "config-file-test".into(),
                "--config.file".into(),
                path.clone().into(),
            ];
            let argv = argv_with_config_file::<TestCli>(literal).expect("no expansion");
            check!(TestCli::try_parse_from(argv).is_err());
        },
    );
}

#[test]
fn an_undefined_expansion_with_no_default_stops_start_up() {
    let _environment = lock_environment();
    let directory = tempfile::tempdir().expect("temp dir");
    let path = write_config(directory.path(), "target: ${KRABKA_TEST_CONFIG_ABSENT}\n");

    temp_env::with_vars([("KRABKA_TEST_CONFIG_ABSENT", None::<&str>)], || {
        let argv: Vec<OsString> = vec![
            "config-file-test".into(),
            "--config.expand-env".into(),
            "--config.file".into(),
            path.clone().into(),
        ];
        assert!(let Err(error) = argv_with_config_file::<TestCli>(argv));
        check!(error.to_string().contains("KRABKA_TEST_CONFIG_ABSENT"));
    });
}
