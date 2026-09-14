use std::{collections::BTreeMap, fmt::Write as _, sync::Arc};

use axum::{
    Extension, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use krabka_observability::server_security::{Principal, authorize_admin};

use super::{MetricStore, PrometheusApiState, authorized_tenant_from_headers};

/// Builds Mimir's tenant-wide ruler inspection and deletion routes.
pub fn mimir_ruler_router<S: MetricStore + 'static>(state: Arc<PrometheusApiState<S>>) -> Router {
    Router::new()
        .route("/ruler/rule_groups", get(all_rule_groups::<S>))
        .route("/ruler/tenants", get(ruler_tenants::<S>))
        .route(
            "/ruler/tenant/{tenant}/rule_groups",
            get(tenant_rule_groups::<S>),
        )
        .route(
            "/ruler/delete_tenant_config",
            post(delete_tenant_config::<S>),
        )
        .with_state(state)
}

async fn all_rule_groups<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
) -> Response {
    if let Err(error) = authorize_admin(&principal) {
        return error.into_response();
    }
    let rules = match state.ruler_rules.read() {
        Ok(rules) => rules
            .iter()
            .map(|(tenant, namespaces)| {
                let namespaces = namespaces
                    .iter()
                    .map(|(namespace, groups)| {
                        (
                            namespace.clone(),
                            groups.values().cloned().collect::<Vec<_>>(),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                (tenant.as_str().to_string(), namespaces)
            })
            .collect::<BTreeMap<_, _>>(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    match serde_yaml::to_string(&rules) {
        Ok(body) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/yaml")],
            body,
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn ruler_tenants<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
) -> Response {
    if let Err(error) = authorize_admin(&principal) {
        return error.into_response();
    }
    let rows = state
        .ruler_tenants()
        .into_iter()
        .fold(String::new(), |mut rows, tenant| {
            write!(rows, "<tr><td>{}</td></tr>", escape_html(tenant.as_str()))
                .expect("writing to a String cannot fail");
            rows
        });
    Html(format!(
        "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"><title>Ruler: bucket tenants</title></head><body><h1>Ruler: bucket tenants</h1><table><tbody>{rows}</tbody></table></body></html>"
    )).into_response()
}

async fn tenant_rule_groups<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    Path(tenant): Path<String>,
) -> Response {
    if let Err(error) = authorize_admin(&principal) {
        return error.into_response();
    }
    let groups = state
        .ruler_rules
        .read()
        .ok()
        .and_then(|rules| {
            rules
                .iter()
                .find(|(id, _)| id.as_str() == tenant)
                .map(|(_, namespaces)| namespaces.clone())
        })
        .unwrap_or_default();
    let rows = groups
        .into_iter()
        .flat_map(|(namespace, groups)| {
            groups
                .into_keys()
                .map(move |name| (namespace.clone(), name))
        })
        .fold(String::new(), |mut rows, (namespace, name)| {
            write!(
                rows,
                "<tr><td>{}</td><td>{}</td></tr>",
                escape_html(&namespace),
                escape_html(&name)
            )
            .expect("writing to a String cannot fail");
            rows
        });
    Html(format!(
        "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"><title>Ruler: tenant rule groups</title></head><body><h1>Ruler: tenant {}</h1><table><tbody>{rows}</tbody></table></body></html>",
        escape_html(&tenant)
    )).into_response()
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

async fn delete_tenant_config<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let deleted = match state.ruler_rules.write() {
        Ok(mut rules) => {
            rules.remove(&tenant);
            true
        }
        Err(_) => false,
    };
    if !deleted {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    match state.persist_ruler_config(&tenant).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    #[test]
    fn operator_html_escapes_rule_identifiers() {
        check!(super::escape_html("<tenant & 'group'>") == "&lt;tenant &amp; &#39;group&#39;&gt;");
    }
}
