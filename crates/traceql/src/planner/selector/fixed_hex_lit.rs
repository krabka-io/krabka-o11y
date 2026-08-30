use super::{Result, TraceqlError};

pub(crate) fn fixed_hex_lit(hex: &str, width: usize) -> Result<String> {
    let expected_len = width * 2;
    if hex.len() != expected_len {
        return Err(TraceqlError::Plan(format!(
            "expected {expected_len} hex characters, got {}",
            hex.len()
        )));
    }
    if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(TraceqlError::Plan(
            "hex string contains non-hex characters".into(),
        ));
    }
    Ok(format!("X'{}'", hex.to_ascii_lowercase()))
}
