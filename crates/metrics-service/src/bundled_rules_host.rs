/// The host name that the ruler uses to post its bundled rule groups to its own listener.
///
/// The HTTP client resolves this name to the listener's address and does not
/// ask DNS. A TLS certificate should be valid for this name.
pub const BUNDLED_RULES_HOST: &str = "localhost";
