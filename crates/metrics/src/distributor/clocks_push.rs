use super::*;

super::ingest_push_handler!(
    clocks_push,
    clocks_push_inner,
    super::respond_ingest_request
);
