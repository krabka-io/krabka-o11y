use super::{AllStage, DebuginfodConfig};

/// The symbolizer, as `--target all` runs it.
///
/// It does not call [`run_with_config`], which is what `--target symbolizer`
/// runs, and the difference matters twice over.
///
/// The first reason is correctness. `run_with_config` builds the resolver and
/// then waits on `SIGTERM` itself. Inside a staged drain that is a second
/// signal handler racing the process's own: on `SIGTERM` this stage would
/// return before its token was ever cancelled, and whether that read as a
/// clean stop or as a role that died on its own would depend on which of the
/// two handlers woke first. A role whose exit code is a coin flip is worse
/// than no role at all.
///
/// The second is that the stage has no work to do in the first place. In this
/// crate -- as in Pyroscope -- symbolization happens inside the read path:
/// the cold store resolves native addresses through the same resolver chain,
/// built from the same `--debuginfod-url` list. So what this stage honestly
/// is, is the configuration proving itself. It builds the resolver, which is
/// where an unusable debuginfod URL is found, holds it for as long as the
/// process serves, and waits. A failure to build ends the stage, which the
/// supervisor reports and the process exits non-zero on -- the same answer
/// `--target symbolizer` gives an operator who mistyped a URL.
///
/// [`run_with_config`]: krabka_profiles::symbolizer::run_with_config
pub(crate) fn symbolizer_stage(urls: Vec<String>, config: DebuginfodConfig) -> AllStage {
    Box::new(move |token| {
        Box::pin(async move {
            match krabka_profiles::symbolizer::native_resolver_from_debuginfod_config(
                urls.clone(),
                config,
            ) {
                Ok(_resolver) => {
                    tracing::info!(
                        debuginfod_urls = ?urls,
                        "profiles symbolizer ready; DWARF/debuginfod resolver is loaded"
                    );
                    token.cancelled().await;
                }
                Err(error) => {
                    tracing::error!(%error, "profiles symbolizer could not build its resolver");
                }
            }
        })
    })
}
