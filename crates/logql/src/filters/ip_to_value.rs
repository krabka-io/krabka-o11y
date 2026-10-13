use super::{IpAddr, IpFamily};

pub(crate) struct IpValue {
    pub(crate) family: IpFamily,
    pub(crate) value: u128,
}

pub(crate) fn ip_to_value(addr: IpAddr) -> IpValue {
    match addr {
        IpAddr::V4(addr) => IpValue {
            family: IpFamily::V4,
            value: u128::from(u32::from(addr)),
        },
        IpAddr::V6(addr) => IpValue {
            family: IpFamily::V6,
            value: u128::from(addr),
        },
    }
}
