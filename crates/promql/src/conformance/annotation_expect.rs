
/// An expected or forbidden annotation on an eval result.
///
/// This enum mirrors the Prometheus promqltest directives `expect warn`,
/// `expect info`, `expect no_warn`, and `expect no_info`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnnotationExpect {
    /// `expect warn`: at least one warning must be raised.
    AnyWarn,
    /// `expect info`: at least one info must be raised.
    AnyInfo,
    /// `expect no_warn`: no warnings may be raised.
    NoWarn,
    /// `expect no_info`: no infos may be raised.
    NoInfo,
    /// `expect warn msg:<text>`: a warning exactly equal to `<text>` must exist.
    WarnMsg(String),
    /// `expect info msg:<text>`: an info exactly equal to `<text>` must exist.
    InfoMsg(String),
    /// `expect ordered`: a result-ordering directive with no annotation semantics.
    Ordered,
}
