use super::{IpAddr, IpRange, ParseError, ip_candidate_tokens, parse_ip_addr};

#[derive(Clone, Debug, Eq, PartialEq)]
/// An IP address, range, or CIDR matcher.
pub struct IpMatcher {
    pub(crate) pattern: String,
    pub(crate) range: IpRange,
}

impl IpMatcher {
    /// Parses an IP address, inclusive range, or CIDR pattern.
    #[tracing::instrument(level = "debug", skip_all, fields(pattern = %pattern), err)]
    /// # Errors
    /// Returns an error when an address, range, or prefix is invalid.
    pub fn parse(pattern: &str) -> Result<Self, ParseError> {
        let range = if let Some(parts) = pattern.split_once('-') {
            IpRange::range(parse_ip_addr(parts.0)?, parse_ip_addr(parts.1)?)?
        } else if let Some(parts) = pattern.split_once('/') {
            let base = parse_ip_addr(parts.0)?;
            let prefix = parts.1.parse::<u8>().map_err(|_| ParseError::Syntax {
                message: "invalid ip CIDR prefix".to_string(),
                position: 0,
            })?;
            IpRange::cidr(base, prefix)?
        } else {
            IpRange::single(parse_ip_addr(pattern)?)
        };

        Ok(Self {
            pattern: pattern.to_string(),
            range,
        })
    }

    #[must_use]
    /// Returns the original matcher pattern.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    #[must_use]
    pub(crate) fn matches_ip_text(&self, value: &str) -> bool {
        value
            .parse::<IpAddr>()
            .is_ok_and(|addr| self.range.contains(addr))
    }

    #[must_use]
    pub(crate) fn matches_line(&self, line: &str) -> bool {
        ip_candidate_tokens(line).any(|candidate| self.matches_ip_text(candidate))
    }
}
