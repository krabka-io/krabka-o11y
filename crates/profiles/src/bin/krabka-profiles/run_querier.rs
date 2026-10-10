use super::{ReadRole, ReadRoleInputs, run_read_role};

/// Answers a query from the WAL tail this role keeps and the blocks its index
/// names.
///
/// `inputs.security` sets the TLS and authentication of `--listen`, and the TLS and
/// SASL of the WAL tail.
///
/// # Errors
/// Returns an error when the object store or the block index cannot be
/// reached, when `--listen` cannot be bound, or when a supervised task ends
/// before the role was asked to stop.
pub(crate) async fn run_querier(inputs: ReadRoleInputs) -> Result<(), Box<dyn std::error::Error>> {
    run_read_role(ReadRole::Querier, inputs).await
}
