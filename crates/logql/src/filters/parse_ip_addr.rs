use super::*;

pub(crate) fn parse_ip_addr(value: &str) -> Result<IpAddr, ParseError> {
    value.parse().map_err(|_| ParseError::Syntax {
        message: "invalid ip pattern".to_string(),
        position: 0,
    })
}
