use axum::{
    body::Bytes,
    extract::RawQuery,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use url::form_urlencoded;

use super::{ApiError, required_form_param, success_data_response};
mod parse_query_params;
use parse_query_params::{ParseQueryParams, parse_query_params};

pub(crate) async fn format_query(RawQuery(raw_query): RawQuery) -> Response {
    match parse_query_params(raw_query.as_deref().unwrap_or_default().as_bytes()) {
        Ok(params) => format_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

pub(crate) fn format_query_inner(params: &ParseQueryParams) -> Response {
    match crate::format_promql_query(&params.query) {
        Ok(formatted) => success_data_response(formatted),
        Err(error) => ApiError::from(error).into_response(),
    }
}

pub(crate) async fn format_query_post(body: Bytes) -> Response {
    match parse_query_params(&body) {
        Ok(params) => format_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn parse_query(RawQuery(raw_query): RawQuery) -> Response {
    match parse_query_params(raw_query.as_deref().unwrap_or_default().as_bytes()) {
        Ok(params) => parse_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

pub(crate) fn parse_query_inner(params: &ParseQueryParams) -> Response {
    match crate::serialize_promql_query(&params.query) {
        Ok(value) => success_data_response(value),
        Err(error) => ApiError::from(error).into_response(),
    }
}

pub(crate) async fn parse_query_post(body: Bytes) -> Response {
    match parse_query_params(&body) {
        Ok(params) => parse_query_inner(&params),
        Err(error) => error.into_response(),
    }
}
