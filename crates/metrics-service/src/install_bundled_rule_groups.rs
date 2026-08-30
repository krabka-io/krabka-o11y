use super::*;

/// Installs every rule group of a bundled rule file into the ruler config store.
///
/// The ruler role calls this once at startup for `--ruler-bundled-rules`. It
/// posts each group to the Mimir ruler-config API of `router`, so a bundled
/// group takes the same validation and the same storage as a group an operator
/// posts. The file stem names the rule namespace, and `/api/v1/rules` renders
/// that namespace as the rule file.
///
/// Returns the name of each installed group, in file order.
///
/// # Errors
///
/// Returns an error when the file is unreadable, when it is not a Prometheus
/// rule file, when it holds no rule group, or when the ruler config API rejects
/// a group. An operator who names a rule file and gets no rules has an alerting
/// gap and no signal, so each of these cases stops the start.
pub async fn install_bundled_rule_groups(
    router: &Router,
    path: &StdPath,
    tenant: &str,
) -> Result<Vec<String>, BundledRulesError> {
    let text = std::fs::read_to_string(path).map_err(|source| BundledRulesError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let file: BundledRuleFile =
        serde_yaml::from_str(&text).map_err(|source| BundledRulesError::Decode {
            path: path.to_path_buf(),
            source,
        })?;
    if file.groups.is_empty() {
        return Err(BundledRulesError::NoGroups {
            path: path.to_path_buf(),
        });
    }
    let namespace = bundled_rules_namespace(path)?;

    let mut installed = Vec::with_capacity(file.groups.len());
    for (index, group) in file.groups.iter().enumerate() {
        let label = bundled_group_label(index, group);
        let body = serde_yaml::to_string(group).map_err(|source| BundledRulesError::Encode {
            group: label.clone(),
            source,
        })?;
        let request = Request::builder()
            .method("POST")
            .uri(format!("/prometheus/config/v1/rules/{namespace}"))
            .header("X-Scope-OrgID", tenant)
            .header(header::CONTENT_TYPE, "application/yaml")
            .body(Body::from(body))
            .map_err(|source| BundledRulesError::Request {
                group: label.clone(),
                source,
            })?;
        // The router answers every request, so its service error is uninhabited.
        let response = match router.clone().oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        };
        let status = response.status();
        if !status.is_success() {
            let body = axum::body::to_bytes(
                response.into_body(),
                BUNDLED_RULES_RESPONSE_MAX.bytes_usize(),
            )
            .await
            .map_err(|source| BundledRulesError::ResponseBody {
                group: label.clone(),
                source,
            })?;
            return Err(BundledRulesError::Rejected {
                group: label,
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        installed.push(label);
    }
    Ok(installed)
}
