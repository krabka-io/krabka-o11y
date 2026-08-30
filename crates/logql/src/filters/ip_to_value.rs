use super::{IpAddr, IpFamily};

pub(crate) fn ip_to_value(addr: IpAddr) -> (IpFamily, u128) {
    match addr {
        IpAddr::V4(addr) => (IpFamily::V4, u128::from(u32::from(addr))),
        IpAddr::V6(addr) => (IpFamily::V6, u128::from(addr)),
    }
}
