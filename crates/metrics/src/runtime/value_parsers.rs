use super::*;

/// Parses the row target a compacted block is left alone at, which is at least
/// one row.
///
/// A target of zero rows would seal every block on sight and the ladder would
/// compact nothing.
pub(crate) fn parse_compactor_target_rows(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        Ok(_) => Err("value must be at least 1".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}

pub(crate) fn parse_distributor_max_decompressed(value: &str) -> Result<ByteSize, String> {
    let size = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    let bytes = size.bytes_f64();
    if bytes.fract() != 0.0 || bytes > 9_007_199_254_740_992.0 {
        return Err(
            "size must be a positive whole-byte value exactly representable by UOM".to_owned(),
        );
    }
    usize::try_from(size.bytes_u64())
        .map_err(|_| "size must fit the platform request boundary".to_owned())?;
    Ok(size)
}

pub(crate) fn parse_ingest_rate_bucket_cap(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| error.to_string())
        .and_then(IngestRateBucketCap::new)
        .map(IngestRateBucketCap::get)
}

/// The `--target` parser: [`Target`] plus an answer for the roles this binary
/// no longer has.
///
/// Wrapping clap's own [`EnumValueParser`] rather than replacing it keeps the
/// possible values in `--help`, which a bare `value_parser` function would
/// drop.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TargetValueParser;

impl TypedValueParser for TargetValueParser {
    type Value = Target;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        if let Some(message) = value.to_str().and_then(retired_role_message) {
            // Cloned because `Command::error` renders the usage and the
            // `--help` pointer, and wants `&mut` to do it. This is the parse
            // failure path: the process is about to exit either way.
            return Err(cmd.clone().error(ErrorKind::InvalidValue, message));
        }
        EnumValueParser::<Target>::new().parse_ref(cmd, arg, value)
    }

    fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
        Some(Box::new(
            Target::value_variants()
                .iter()
                .filter_map(ValueEnum::to_possible_value),
        ))
    }
}

/// What `--target` says about a role this binary used to have, and no longer.
///
/// `querier`, `query-frontend` and `ruler` were once targets of
/// `krabka-metrics`. Each bound the data port, logged that it was listening,
/// answered `/ready` and served a build-info route -- enough for a Grafana
/// datasource to read as healthy -- and then 404'd every query. They are gone,
/// and the roles under those names live in `krabka-metrics-service`.
///
/// Clap's own rejection would say only that the value is not one of the
/// variants. An operator holding a deployment that names `querier` would learn
/// from that which binary does *not* serve it and nothing about which one
/// does, so this message names the binary instead.
///
/// The roles it offers instead are read off [`Target`], not written out here,
/// so a renamed role cannot leave this message pointing at a name no longer
/// accepted -- which is the same failure one layer down from the one the
/// retirement fixed.
///
/// Returns `None` for any other value, which is the parser's cue to go on and
/// match it against [`Target`] as usual.
///
/// [`Target`]: super::Target
pub(crate) fn retired_role_message(value: &str) -> Option<String> {
    if !matches!(value, "querier" | "query-frontend" | "ruler") {
        return None;
    }
    let live: Vec<String> = Target::value_variants()
        .iter()
        .filter_map(ValueEnum::to_possible_value)
        .map(|role| format!("--target={}", role.get_name()))
        .collect();
    Some(format!(
        "`{value}` is not a role of krabka-metrics\n\n  \
         krabka-metrics is the metrics write path: {}.\n  \
         The read path is a second binary. Run `krabka-metrics-service --target={value}` for it.",
        live.join(" and ")
    ))
}
