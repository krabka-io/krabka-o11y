use super::WireError;

pub(crate) fn hex_fixed<const N: usize>(hex: &str) -> Result<[u8; N], WireError> {
    if hex.is_empty() || !hex.len().is_multiple_of(2) || hex.len() > N * 2 {
        return Err(WireError::Invalid(format!("bad hex id {hex:?}")));
    }

    let bytes = hex::decode(hex).map_err(|err| WireError::Invalid(err.to_string()))?;
    let mut out = [0; N];
    out[N - bytes.len()..].copy_from_slice(&bytes);
    Ok(out)
}
