use super::{ByteSize, parse};

pub(crate) fn parse_frame_max(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    krabka_client_core::ClientFrameMax::try_from(value)
        .map(krabka_client_core::ClientFrameMax::size)
}
