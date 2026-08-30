use super::{IpAddr, IpRange, ParseError, ip_candidate_tokens, parse_ip_addr};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IpMatcher {
    pub(crate) pattern: String,
    pub(crate) range: IpRange,
}

impl IpMatcher {
    #[tracing::instrument(level = "debug", skip_all, fields(pattern = %pattern), err)]
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn parse(pattern: &str) -> Result<Self, ParseError> {
        let range = if let Some((start, end)) = pattern.split_once('-') {
            IpRange::range(parse_ip_addr(start)?, parse_ip_addr(end)?)?
        } else if let Some((base, prefix)) = pattern.split_once('/') {
            let base = parse_ip_addr(base)?;
            let prefix = prefix.parse::<u8>().map_err(|_| ParseError::Syntax {
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
