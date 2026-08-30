use super::*;

pub(crate) fn form_body_query(body: &Bytes) -> Result<String, HttpQueryError> {
    String::from_utf8(body.to_vec()).map_err(|_| HttpQueryError::InvalidPercentEncoding)
}
