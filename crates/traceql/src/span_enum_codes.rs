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
    #[test]
    fn span_kind_and_status_names_map_to_their_stored_numbers() {
        let kinds = [
            ("unspecified", 0),
            ("internal", 1),
            ("server", 2),
            ("client", 3),
            ("producer", 4),
            ("consumer", 5),
        ];
        for (name, value) in kinds {
            check!(kind_enum_value(name) == Some(value), "kind {name}");
        }
        check!(
            kind_enum_value("Server") == None,
            "the match is case-sensitive"
        );
        check!(kind_enum_value("") == None);
        check!(
            kind_enum_value("gateway") == None,
            "an unknown kind is not a number"
        );

        for (name, value) in [("unset", 0), ("ok", 1), ("error", 2)] {
            check!(status_enum_value(name) == Some(value), "status {name}");
        }
        check!(
            status_enum_value("OK") == None,
            "the match is case-sensitive"
        );
        check!(status_enum_value("failed") == None);
    }
}
