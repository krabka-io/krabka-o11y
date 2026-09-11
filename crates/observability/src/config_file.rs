//! A YAML configuration file for the service binaries.
//!
//! Five binaries in this workspace carry between twenty and sixty flags, each
//! with an environment binding. At that size a deployment is a wall of
//! `KRABKA_*` variables that nothing can review, diff or check into a
//! repository, so this module lets one file stand in for the flags.
//!
//! The file needs no schema of its own. Its keys are the binary's own long
//! flag names, and its values go back through the same `clap` value parsers
//! the flags use, so there is one definition of the configuration and it
//! cannot drift from the one `--help` prints. `listen_addr` and `listen-addr`
//! both name `--listen-addr`; a key that names no flag is an error rather than
//! a line the process ignores.
//!
//! ## Precedence
//!
//! Highest wins:
//!
//! 1. the command line,
//! 2. the environment,
//! 3. the config file,
//! 4. the flag's default.
//!
//! This is the order Loki, Mimir and Tempo use, and the order an operator
//! expects: the file is the reviewed baseline, and a flag is the deliberate
//! override in front of it. [`argv_with_config_file`] enforces it by asking
//! `clap` where each value came from and supplying a file value only for the
//! arguments that neither the command line nor the environment set.

use crate::{
    ArgAction, ArgMatches, Command, CommandFactory, Error, FsPath, OsString, Parser, PathBuf,
    ValueSource, YamlValue,
};

mod argv_with_config_file;
mod config_file_args;
mod config_file_error;
mod expand_config_env;
mod file_argument_overrides;
mod yaml_scalar;

pub use argv_with_config_file::argv_with_config_file;
pub use config_file_args::ConfigFileArgs;
pub use config_file_error::ConfigFileError;
pub(crate) use expand_config_env::expand_config_env;
pub(crate) use file_argument_overrides::file_argument_overrides;
pub(crate) use yaml_scalar::yaml_scalar;
