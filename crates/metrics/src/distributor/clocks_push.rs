use super::{
    Arc, BodyBytes, DistributorState, Extension, HeaderMap, IngestRequestStart, Principal,
    Response, State, clocks_push_inner,
};

super::ingest_push_handler!(
    clocks_push,
    clocks_push_inner,
    super::respond_ingest_request
);
