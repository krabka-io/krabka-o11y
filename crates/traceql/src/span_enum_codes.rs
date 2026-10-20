//! Integer codes of the `kind` and `status` span intrinsics, as OTLP encodes them.

/// The OTLP code of a lower-case span kind name, such as `server`.
#[must_use]
pub fn kind_enum_value(name: &str) -> Option<i32> {
    match name {
        "unspecified" => Some(0),
        "internal" => Some(1),
        "server" => Some(2),
        "client" => Some(3),
        "producer" => Some(4),
        "consumer" => Some(5),
        _ => None,
    }
}

/// The OTLP code of a lower-case span status name, such as `error`.
#[must_use]
pub fn status_enum_value(name: &str) -> Option<i32> {
    match name {
        "unset" => Some(0),
        "ok" => Some(1),
        "error" => Some(2),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// Span kind and status names come off the wire as strings and have to
    /// land on the numbers the stored spans use. Every name is checked, since
    /// a table is exactly where an off-by-one goes unnoticed.
    #[yare::parameterized(
        unspecified = { "unspecified", 0 },
        internal = { "internal", 1 },
        server = { "server", 2 },
        client = { "client", 3 },
        producer = { "producer", 4 },
        consumer = { "consumer", 5 },
    )]
    fn span_kind_names_map_to_their_stored_numbers(name: &str, value: i32) {
        assert2::check!(kind_enum_value(name) == Some(value), "kind {name}");
    }

    #[test]
    fn unknown_span_kind_names_are_rejected() {
        check!(
            kind_enum_value("Server") == None,
            "the match is case-sensitive"
        );
        check!(kind_enum_value("") == None);
        check!(
            kind_enum_value("gateway") == None,
            "an unknown kind is not a number"
        );
    }

    #[yare::parameterized(
        unset = { "unset", 0 },
        ok = { "ok", 1 },
        error = { "error", 2 },
    )]
    fn span_status_names_map_to_their_stored_numbers(name: &str, value: i32) {
        assert2::check!(status_enum_value(name) == Some(value), "status {name}");
    }

    #[test]
    fn unknown_span_status_names_are_rejected() {
        check!(
            status_enum_value("OK") == None,
            "the match is case-sensitive"
        );
        check!(status_enum_value("failed") == None);
    }
}
