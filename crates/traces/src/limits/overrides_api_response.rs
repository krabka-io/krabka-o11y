use axum::{
    body::Body,
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use super::{OverrideMutationError, OverridesProvider};

pub(crate) fn overrides_api_response(
    overrides: &OverridesProvider,
    tenant: &str,
    method: &Method,
    headers: &HeaderMap,
    query: Option<&str>,
    body: &Bytes,
) -> Response {
    match *method {
        Method::GET => get(overrides, tenant, query),
        Method::POST => post(overrides, tenant, headers, query, body),
        Method::PATCH => patch(overrides, tenant, query, body),
        Method::DELETE => delete(overrides, tenant, headers),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

fn get(overrides: &OverridesProvider, tenant: &str, query: Option<&str>) -> Response {
    let scope = query
        .and_then(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == "scope")
                .map(|(_, value)| value.into_owned())
        })
        .unwrap_or_else(|| "api".to_string());
    if scope != "api" && scope != "merged" {
        return text_error(
            StatusCode::BAD_REQUEST,
            &format!("unknown scope \"{scope}\", valid options are api and merged"),
        );
    }
    match overrides.api_get(tenant) {
        Some((limits, version)) => json_response(
            if scope == "merged" {
                serde_json::to_value(overrides.for_tenant(tenant)).unwrap_or(limits)
            } else {
                limits
            },
            Some(version),
        ),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn post(
    overrides: &OverridesProvider,
    tenant: &str,
    headers: &HeaderMap,
    query: Option<&str>,
    body: &Bytes,
) -> Response {
    let Some(expected) = headers
        .get("if-match")
        .and_then(|value| value.to_str().ok())
    else {
        return text_error(
            StatusCode::PRECONDITION_REQUIRED,
            "must specify If-Match header",
        );
    };
    if let Some(response) = invalid_skip_conflicts(query) {
        return response;
    }
    let raw = match serde_json::from_slice(body) {
        Ok(raw) => raw,
        Err(error) => return text_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    match overrides.api_set(tenant, raw, expected) {
        Ok(version) => empty_response(StatusCode::OK, Some(version)),
        Err(error) => mutation_error(&error),
    }
}

fn patch(
    overrides: &OverridesProvider,
    tenant: &str,
    query: Option<&str>,
    body: &Bytes,
) -> Response {
    if let Some(response) = invalid_skip_conflicts(query) {
        return response;
    }
    let patch = match serde_json::from_slice(body) {
        Ok(patch) => patch,
        Err(error) => return text_error(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    match overrides.api_patch(tenant, patch) {
        Ok((limits, version)) => json_response(limits, Some(version)),
        Err(error) => mutation_error(&error),
    }
}

fn invalid_skip_conflicts(query: Option<&str>) -> Option<Response> {
    let value = query.and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == "skip-conflicting-overrides-check")
            .map(|(_, value)| value)
    });
    if value.is_some_and(|value| value.parse::<bool>().is_err()) {
        return Some(text_error(
            StatusCode::BAD_REQUEST,
            "could not parse skip-conflicting-overrides-check, must be a boolean value",
        ));
    }
    None
}

fn delete(overrides: &OverridesProvider, tenant: &str, headers: &HeaderMap) -> Response {
    let Some(expected) = headers
        .get("if-match")
        .and_then(|value| value.to_str().ok())
    else {
        return text_error(
            StatusCode::PRECONDITION_REQUIRED,
            "must specify If-Match header",
        );
    };
    match overrides.api_delete(tenant, expected) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => mutation_error(&error),
    }
}

fn mutation_error(error: &OverrideMutationError) -> Response {
    let status = match error {
        OverrideMutationError::NotFound => StatusCode::NOT_FOUND,
        OverrideMutationError::VersionMismatch => StatusCode::PRECONDITION_FAILED,
        OverrideMutationError::Invalid(_) => StatusCode::BAD_REQUEST,
    };
    text_error(status, &error.to_string())
}

fn json_response(value: impl serde::Serialize, version: Option<String>) -> Response {
    let Ok(body) = serde_json::to_vec(&value) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    if let Some(version) = version.and_then(|version| version.parse().ok()) {
        response.headers_mut().insert(header::ETAG, version);
    }
    response
}

fn empty_response(status: StatusCode, version: Option<String>) -> Response {
    let mut response = status.into_response();
    if let Some(version) = version.and_then(|version| version.parse().ok()) {
        response.headers_mut().insert(header::ETAG, version);
    }
    response
}

fn text_error(status: StatusCode, message: &str) -> Response {
    (status, format!("{message}\n")).into_response()
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt as _;

    use super::*;
    use crate::limits::Limits;

    #[tokio::test]
    async fn mutations_follow_tempo_preconditions_and_etags() {
        let overrides = OverridesProvider::new(Limits::default());
        let mut headers = HeaderMap::new();

        let missing = overrides_api_response(
            &overrides,
            "tenant-a",
            &Method::POST,
            &headers,
            None,
            &Bytes::from_static(br#"{"max_spans_per_trace":7}"#),
        );
        assert2::check!(missing.status() == StatusCode::PRECONDITION_REQUIRED);

        let invalid_query = overrides_api_response(
            &overrides,
            "tenant-a",
            &Method::PATCH,
            &headers,
            Some("skip-conflicting-overrides-check=maybe"),
            &Bytes::from_static(b"{}"),
        );
        assert2::check!(invalid_query.status() == StatusCode::BAD_REQUEST);

        headers.insert("if-match", "0".parse().unwrap());
        let created = overrides_api_response(
            &overrides,
            "tenant-a",
            &Method::POST,
            &headers,
            None,
            &Bytes::from_static(br#"{"max_spans_per_trace":7}"#),
        );
        assert2::check!(created.status() == StatusCode::OK);
        assert2::check!(created.headers()[header::ETAG] == "1");

        let patched = overrides_api_response(
            &overrides,
            "tenant-a",
            &Method::PATCH,
            &HeaderMap::new(),
            None,
            &Bytes::from_static(br#"{"max_traces_per_search":9}"#),
        );
        assert2::check!(patched.status() == StatusCode::OK);
        assert2::check!(patched.headers()[header::ETAG] == "2");
        let body = patched.into_body().collect().await.unwrap().to_bytes();
        assert2::check!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()
                == serde_json::json!({
                    "max_spans_per_trace": 7,
                    "max_traces_per_search": 9,
                })
        );
    }
}
