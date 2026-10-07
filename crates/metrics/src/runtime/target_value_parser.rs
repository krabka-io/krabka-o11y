use super::{
    Arg, Command, EnumValueParser, Error, ErrorKind, OsStr, PossibleValue, Target,
    TypedValueParser, ValueEnum, retired_role_message,
};

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
