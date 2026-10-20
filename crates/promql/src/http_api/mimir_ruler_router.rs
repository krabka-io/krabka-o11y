use std::{collections::BTreeMap, fmt::Write as _, sync::Arc};

use axum::{
    Extension, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use krabka_observability::server_security::{Principal, authorize_admin};

use super::{AuthorizedTenant, MetricStore, PrometheusApiState};

#[allow(clippy::result_large_err)]
fn require_ruler_admin(principal: &Principal) -> Result<(), Response> {
    authorize_admin(principal).map_err(axum::response::IntoResponse::into_response)
}

macro_rules! ruler_admin_handler {
    ($name:ident; [$($extractors:tt)*]; $state:ident, $principal:ident; $body:block) => {
        async fn $name<S: MetricStore>(
            State($state): State<Arc<PrometheusApiState<S>>>,
            Extension($principal): Extension<Principal>,
            $($extractors)*
        ) -> Response {
            if let Err(response) = require_ruler_admin(&$principal) {
                return response;
            }
            $body
        }
    };
}

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

ruler_admin_handler!(all_rule_groups; []; state, principal; {
    let rules = match state.ruler_rules.read() {
        Ok(rules) => {
            let mut output = BTreeMap::new();
            for tenant_entry in rules.iter() {
                let mut namespaces_output = BTreeMap::new();
                for namespace_entry in tenant_entry.1 {
                    namespaces_output.insert(
                        namespace_entry.0.clone(),
                        namespace_entry.1.values().cloned().collect::<Vec<_>>(),
                    );
                }
                output.insert(tenant_entry.0.as_str().to_string(), namespaces_output);
            }
            output
        }
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    match serde_yaml::to_string(&rules) {
        Ok(body) => {
            let mut response = super::response_with_status(StatusCode::OK, body);
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/yaml"),
            );
            response
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
});

ruler_admin_handler!(ruler_tenants; []; state, principal; {
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
});

ruler_admin_handler!(tenant_rule_groups; [Path(tenant): Path<String>,]; state, principal; {
    let groups = state
        .ruler_rules
        .read()
        .ok()
        .and_then(|rules| {
            rules
                .iter()
                .find(|entry| entry.0.as_str() == tenant)
                .map(|entry| entry.1.clone())
        })
        .unwrap_or_default();
    let mut rows = String::new();
    for namespace_entry in groups {
        for name in namespace_entry.1.into_keys() {
            write!(
                rows,
                "<tr><td>{}</td><td>{}</td></tr>",
                escape_html(&namespace_entry.0),
                escape_html(&name)
            )
            .expect("writing to a String cannot fail");
        }
    }
    Html(format!(
        "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"><title>Ruler: tenant rule groups</title></head><body><h1>Ruler: tenant {}</h1><table><tbody>{rows}</tbody></table></body></html>",
        escape_html(&tenant)
    )).into_response()
});

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
    AuthorizedTenant(tenant): AuthorizedTenant,
) -> Response {
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
        Err(error) => super::response_with_status(StatusCode::INTERNAL_SERVER_ERROR, error),
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
