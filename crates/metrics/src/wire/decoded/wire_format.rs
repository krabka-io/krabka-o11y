
/// Which `remote_write` protobuf shape an HTTP request carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireFormat {
    RemoteWriteV1,
    RemoteWriteV2,
}
