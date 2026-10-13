use super::{IpAddr, IpFamily, ParseError, ip_to_value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IpRange {
    pub(crate) family: IpFamily,
    pub(crate) start: u128,
    pub(crate) end: u128,
}

impl IpRange {
    pub(crate) fn single(addr: IpAddr) -> Self {
        let parsed = ip_to_value(addr);
        Self {
            family: parsed.family,
            start: parsed.value,
            end: parsed.value,
        }
    }

    pub(crate) fn range(start: IpAddr, end: IpAddr) -> Result<Self, ParseError> {
        let start = ip_to_value(start);
        let end = ip_to_value(end);
        if start.family != end.family || start.value > end.value {
            return Err(ParseError::Syntax {
                message: "invalid ip range".to_string(),
                position: 0,
            });
        }
        Ok(Self {
            family: start.family,
            start: start.value,
            end: end.value,
        })
    }

    pub(crate) fn cidr(base: IpAddr, prefix: u8) -> Result<Self, ParseError> {
        let parsed = ip_to_value(base);
        let bits = parsed.family.bits();
        if prefix > bits {
            return Err(ParseError::Syntax {
                message: "invalid ip CIDR prefix".to_string(),
                position: 0,
            });
        }
        let host_bits = bits - prefix;
        let mask = if prefix == 0 {
            0
        } else {
            (!0_u128) << u32::from(host_bits)
        } & parsed.family.mask();
        let start = parsed.value & mask;
        let host_mask = !mask & parsed.family.mask();
        let end = start.saturating_add(host_mask);
        Ok(Self {
            family: parsed.family,
            start,
            end,
        })
    }

    pub(crate) fn contains(&self, addr: IpAddr) -> bool {
        let parsed = ip_to_value(addr);
        parsed.family == self.family && parsed.value >= self.start && parsed.value <= self.end
    }
}
