// cargo-mutants: shared decoder guard is covered through remote_write and remote_read callers.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn snappy_block_decode_raw<E>(
    body: &[u8],
    max_output: usize,
    snappy_decode: impl Fn(String) -> E,
    output_too_large: impl Fn(usize) -> E,
) -> Result<Vec<u8>, E> {
    let declared =
        snap::raw::decompress_len(body).map_err(|error| snappy_decode(error.to_string()))?;
    if declared > max_output {
        return Err(output_too_large(max_output));
    }
    let out = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|error| snappy_decode(error.to_string()))?;
    if out.len() > max_output {
        return Err(output_too_large(max_output));
    }
    Ok(out)
}
