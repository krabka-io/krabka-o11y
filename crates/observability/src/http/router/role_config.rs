use super::*;

pub(crate) async fn role_config(RawQuery(raw_query): RawQuery) -> Response {
    status_config(raw_query.as_deref())
}
