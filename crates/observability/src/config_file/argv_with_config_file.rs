use super::{
    CommandFactory, ConfigFileError, OsString, PathBuf, YamlValue, expand_config_env,
    file_argument_overrides,
};

/// Rewrites `argv` so that a `--config.file` supplies the arguments nothing
/// else set.
///
/// Call it in place of [`std::env::args_os`] and hand the result to
/// `Cli::parse_from`. `C` is the binary's own `clap` command, so the file is
/// checked against the flags that binary actually has -- an unknown key stops
/// start-up instead of being dropped.
///
/// The file's arguments go in immediately after the program name, ahead of
/// everything the caller passed, so a `--` on the real command line still ends
/// the options it was written to end.
///
/// # Errors
/// Returns an error when the file cannot be read or parsed, when a key names
/// no flag of `C`, or when a value is not a kind a flag can take.
pub fn argv_with_config_file<C>(
    argv: impl IntoIterator<Item = OsString>,
) -> Result<Vec<OsString>, ConfigFileError>
where
    C: CommandFactory,
{
    let mut argv: Vec<OsString> = argv.into_iter().collect();
    let command = C::command();
    // A tolerant first pass. It exists only to learn which arguments the
    // command line and the environment already supplied, so every error it
    // could raise is the real parse's to report with the real message -- and
    // an argument the file is about to supply is still `required` here, which
    // is why the requirement is lifted for this pass alone.
    let Ok(matches) = command
        .clone()
        .ignore_errors(true)
        .mut_args(|argument| argument.required(false))
        .try_get_matches_from(argv.clone())
    else {
        return Ok(argv);
    };
    let Ok(Some(path)) = matches.try_get_one::<PathBuf>("config_file") else {
        return Ok(argv);
    };
    let path = path.clone();

    let text = std::fs::read_to_string(&path).map_err(|source| ConfigFileError::Read {
        path: path.clone(),
        source,
    })?;
    let text = if matches
        .try_get_one::<bool>("config_expand_env")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(false)
    {
        expand_config_env(&path, &text, &|name| std::env::var(name).ok())?
    } else {
        text
    };
    let document: YamlValue =
        serde_yaml::from_str(&text).map_err(|source| ConfigFileError::Parse {
            path: path.clone(),
            source,
        })?;

    let from_file = file_argument_overrides(&command, &matches, &path, &document)?;
    let at = usize::from(!argv.is_empty());
    argv.splice(at..at, from_file);
    Ok(argv)
}
