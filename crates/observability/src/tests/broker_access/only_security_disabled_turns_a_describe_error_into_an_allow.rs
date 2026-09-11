use super::*;

fn broker_error_for_test(api: &'static str, code: i16) -> AdminError {
    AdminError::Broker {
        api,
        code,
        name: "TEST",
        message: None,
    }
}

/// Kafka error 54 is `SECURITY_DISABLED`: the broker has no authorizer to
/// consult. Every other failure, an authorization failure among them, must
/// stay a failure, so the check fails closed.
#[test]
pub(crate) fn only_security_disabled_turns_a_describe_error_into_an_allow() {
    check!(
        acl_set_from_describe(Err(broker_error_for_test("DescribeAcls", 54))).ok()
            == Some(AclSet::SecurityDisabled)
    );
    check!(acl_set_from_describe(Ok(Vec::new())).ok() == Some(AclSet::Configured(Vec::new())));
    for (name, error) in [
        (
            "cluster authorization failed",
            broker_error_for_test("DescribeAcls", 31),
        ),
        (
            "error 54 from another API",
            broker_error_for_test("DescribeClientQuotas", 54),
        ),
        ("no broker reachable", AdminError::Connect { tried: 1 }),
    ] {
        check!(acl_set_from_describe(Err(error)).is_err(), "{name}");
    }
}
