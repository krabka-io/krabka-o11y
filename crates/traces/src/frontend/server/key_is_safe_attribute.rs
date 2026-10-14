/// A legacy `tags=` key is a safe `TraceQL` attribute reference only if it is
/// made of identifier characters: alphanumerics plus `._:-`.
///
/// Any other character, such as `{`, `}`, `"`, `\`, `|`, `&`, `=` or
/// whitespace, could inject query structure once it is interpolated unquoted
/// into the generated `TraceQL`. This stays in sync with the querier's
/// `key_is_safe_attribute`.
pub(crate) use krabka_traceql::key_is_safe_attribute;
