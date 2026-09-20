use krabka_client_coordination::{
    CoordinationError, CoordinationTransport, Decision, FencingToken, LeaseConfig, MemberId, Role,
    RoleState, evaluate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RulerLeaseState {
    Active {
        token: FencingToken,
        acquired: bool,
        renew_at_ms: i64,
    },
    Standby {
        wake_at_ms: i64,
    },
}

/// Advances one durable ruler lease. The broker epoch carried by `token`, not
/// the deadline, is the write fence.
///
/// # Errors
/// Returns the coordination transport's read, registration, epoch, or lease error.
pub async fn advance_ruler_lease<T: CoordinationTransport>(
    transport: &T,
    role: &Role,
    member: &MemberId,
    held_token: Option<FencingToken>,
    config: LeaseConfig,
    now_ms: i64,
) -> Result<RulerLeaseState, CoordinationError> {
    let state = RoleState::from_records(role.clone(), transport.read_role_records(role).await?);
    match evaluate(&state, member, now_ms, &config) {
        Decision::NotRegistered => {
            transport.register(role, member).await?;
            Ok(RulerLeaseState::Standby { wake_at_ms: now_ms })
        }
        Decision::Wait { until_millis } => Ok(RulerLeaseState::Standby {
            wake_at_ms: until_millis,
        }),
        Decision::Challenge => {
            let token = transport.acquire_epoch(role).await?;
            let lease = config.grant(member.clone(), token, now_ms);
            transport.write_lease(role, token, &lease).await?;
            Ok(RulerLeaseState::Active {
                token,
                acquired: true,
                renew_at_ms: config.timing(&lease).renew_at_millis(),
            })
        }
        Decision::Hold => {
            let Some(lease) = state.lease else {
                return Ok(RulerLeaseState::Standby { wake_at_ms: now_ms });
            };
            let Some(token) = held_token.filter(|token| *token == lease.token) else {
                return Ok(RulerLeaseState::Standby {
                    wake_at_ms: lease.deadline,
                });
            };
            let lease = if config.timing(&lease).renew_due_at(now_ms) {
                let renewed = config.grant(member.clone(), token, now_ms);
                transport.write_lease(role, token, &renewed).await?;
                renewed
            } else {
                lease
            };
            Ok(RulerLeaseState::Active {
                token,
                acquired: false,
                renew_at_ms: config.timing(&lease).renew_at_millis(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use krabka_client_coordination::{
        CoordinationKey, CoordinationRecord, Lease, Registration, RoleRecords,
    };

    use super::*;

    #[derive(Default)]
    struct FakeTransport {
        records: Mutex<RoleRecords>,
        current: Mutex<Option<FencingToken>>,
        next_epoch: Mutex<i16>,
        fail_reads: Mutex<bool>,
    }

    #[async_trait]
    impl CoordinationTransport for FakeTransport {
        async fn acquire_epoch(&self, _role: &Role) -> Result<FencingToken, CoordinationError> {
            let mut epoch = self.next_epoch.lock().unwrap();
            let token = FencingToken::new(7, *epoch)?;
            *epoch += 1;
            *self.current.lock().unwrap() = Some(token);
            Ok(token)
        }

        async fn read_role_records(&self, _role: &Role) -> Result<RoleRecords, CoordinationError> {
            if *self.fail_reads.lock().unwrap() {
                return Err(CoordinationError::InvalidConfig(
                    "simulated broker partition".to_owned(),
                ));
            }
            Ok(self.records.lock().unwrap().clone())
        }

        async fn register(&self, role: &Role, member: &MemberId) -> Result<(), CoordinationError> {
            let mut records = self.records.lock().unwrap();
            let offset = i64::try_from(records.len()).unwrap();
            records.push((
                offset,
                CoordinationKey::registration(role.clone(), member.clone()),
                CoordinationRecord::Registration(Registration {
                    member: member.clone(),
                    registered_at: 0,
                }),
            ));
            Ok(())
        }

        async fn write_lease(
            &self,
            role: &Role,
            token: FencingToken,
            lease: &Lease,
        ) -> Result<(), CoordinationError> {
            if *self.current.lock().unwrap() != Some(token) {
                return Err(CoordinationError::Fenced { role: role.clone() });
            }
            let mut records = self.records.lock().unwrap();
            let offset = i64::try_from(records.len()).unwrap();
            records.push((
                offset,
                CoordinationKey::lease(role.clone()),
                CoordinationRecord::Lease(lease.clone()),
            ));
            Ok(())
        }

        async fn describe(&self, _role: &Role) -> Result<Option<FencingToken>, CoordinationError> {
            Ok(*self.current.lock().unwrap())
        }
    }

    fn role() -> Role {
        Role::new("ruler-1").unwrap()
    }

    fn member(name: &str) -> MemberId {
        MemberId::new(name).unwrap()
    }

    #[tokio::test]
    async fn expired_holder_is_broker_fenced_before_takeover_writes() {
        let transport = FakeTransport::default();
        let config = LeaseConfig::default();
        let first = member("first");
        let second = member("second");

        assert!(matches!(
            advance_ruler_lease(&transport, &role(), &first, None, config, 0)
                .await
                .unwrap(),
            RulerLeaseState::Standby { .. }
        ));
        let first_token = match advance_ruler_lease(&transport, &role(), &first, None, config, 0)
            .await
            .unwrap()
        {
            RulerLeaseState::Active { token, .. } => token,
            state @ RulerLeaseState::Standby { .. } => {
                panic!("expected active first member, got {state:?}")
            }
        };
        advance_ruler_lease(&transport, &role(), &second, None, config, 0)
            .await
            .unwrap();
        let second_token =
            match advance_ruler_lease(&transport, &role(), &second, None, config, 30_000)
                .await
                .unwrap()
            {
                RulerLeaseState::Active { token, .. } => token,
                state @ RulerLeaseState::Standby { .. } => {
                    panic!("expected active second member, got {state:?}")
                }
            };

        assert!(second_token > first_token);
        let stale = config.grant(first, first_token, 30_001);
        assert!(
            transport
                .write_lease(&role(), first_token, &stale)
                .await
                .unwrap_err()
                .is_fenced()
        );
    }

    #[tokio::test]
    async fn broker_partition_never_grants_local_authority() {
        let transport = FakeTransport::default();
        *transport.fail_reads.lock().unwrap() = true;
        let error = advance_ruler_lease(
            &transport,
            &role(),
            &member("isolated"),
            None,
            LeaseConfig::default(),
            i64::MAX,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("simulated broker partition"));
    }
}
