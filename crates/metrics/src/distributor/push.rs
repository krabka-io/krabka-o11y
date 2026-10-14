use super::{
    Arc, BodyBytes, DistributorState, Extension, HeaderMap, IngestRequestStart, Principal,
    Response, State, push_inner,
};

super::ingest_push_handler!(push, push_inner, super::respond_ingest_request);
