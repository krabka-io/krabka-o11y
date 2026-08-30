use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IpRange {
    pub(crate) family: IpFamily,
    pub(crate) start: u128,
    pub(crate) end: u128,
}

impl IpRange {
    pub(crate) fn single(addr: IpAddr) -> Self {
        let (family, value) = ip_to_value(addr);
        Self {
            family,
            start: value,
            end: value,
        }
    }

    pub(crate) fn range(start: IpAddr, end: IpAddr) -> Result<Self, ParseError> {
        let (start_family, start) = ip_to_value(start);
        let (end_family, end) = ip_to_value(end);
        if start_family != end_family || start > end {
            return Err(ParseError::Syntax {
                message: "invalid ip range".to_string(),
                position: 0,
            });
        }
        Ok(Self {
            family: start_family,
            start,
            end,
        })
    }

    pub(crate) fn cidr(base: IpAddr, prefix: u8) -> Result<Self, ParseError> {
        let (family, value) = ip_to_value(base);
        let bits = family.bits();
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
        } & family.mask();
        let start = value & mask;
        let host_mask = !mask & family.mask();
        let end = start.saturating_add(host_mask);
        Ok(Self { family, start, end })
    }

    pub(crate) fn contains(&self, addr: IpAddr) -> bool {
        let (family, value) = ip_to_value(addr);
        family == self.family && value >= self.start && value <= self.end
    }
}
