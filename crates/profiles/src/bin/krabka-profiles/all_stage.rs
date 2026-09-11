use super::CancellationToken;

/// One role of `--target all`, as the staged drain runs it: a future that is
/// handed the token which stops that role, and nothing else.
///
/// Boxed because the six roles have six unrelated future types and the drain
/// stores them in one ordered collection. The alternative -- six typed fields
/// taken one at a time -- puts the stop order in the shape of a struct, where
/// changing it is a refactor rather than an edit to a list.
pub(crate) type AllStage =
    Box<dyn FnOnce(CancellationToken) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Send>;
