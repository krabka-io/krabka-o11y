use super::{AclEntry, AclSet, AdminError};

/// Classifies one `DescribeAcls` answer.
///
/// Only Kafka error 54, `SECURITY_DISABLED`, means that the broker has no
/// authorizer. Every other error stays an error, so a check that cannot reach
/// the ACLs fails closed.
pub(crate) fn acl_set_from_describe(
    result: Result<Vec<AclEntry>, AdminError>,
) -> Result<AclSet, AdminError> {
    match result {
        Ok(entries) => Ok(AclSet::Configured(entries)),
        Err(AdminError::Broker {
            api: "DescribeAcls",
            code: 54,
            ..
        }) => Ok(AclSet::SecurityDisabled),
        Err(error) => Err(error),
    }
}
