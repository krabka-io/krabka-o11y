use krabka_domain_macros::EnumName;

/// The URL scheme that the query-frontend dials its queriers with.
///
/// `--querier-url` names it, and every URL in the flag has the same scheme. A
/// querier that serves TLS is dialled with [`QuerierScheme::Https`], and the
/// frontend then checks the querier certificate against the roots of the
/// internal client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, EnumName)]
pub enum QuerierScheme {
    /// Plain HTTP. Default.
    #[default]
    #[name(value = "http")]
    Http,
    /// HTTP over TLS.
    #[name(value = "https")]
    Https,
}

impl QuerierScheme {
    /// The scheme of a listener that serves TLS when `tls` is true.
    #[must_use]
    pub const fn serving_tls(tls: bool) -> Self {
        if tls { Self::Https } else { Self::Http }
    }
}
