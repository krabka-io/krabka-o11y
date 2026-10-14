macro_rules! impl_recorded_security_events {
    ($source_view:expr) => {
        impl SecurityEvents for RecordedEvents {
            fn authentication_failed(
                &self,
                source: Option<SocketAddr>,
                attempted: Option<AuthMethod>,
                reason: AuthFailureReason,
            ) {
                let source = ($source_view)(source);
                self.push(format!("failed {source:?} {attempted:?} {reason:?}"));
            }

            fn authentication_succeeded(
                &self,
                source: Option<SocketAddr>,
                principal: &str,
                method: AuthMethod,
            ) {
                let source = ($source_view)(source);
                self.push(format!("succeeded {source:?} {principal} {method:?}"));
            }

            fn tenant_denied(&self, principal: &str, _method: AuthMethod, tenant: &TenantId) {
                self.push_tenant_denied(principal, tenant);
            }

            fn admin_denied(&self, principal: &str, _method: AuthMethod) {
                self.push_admin_denied(principal);
            }
        }
    };
}
