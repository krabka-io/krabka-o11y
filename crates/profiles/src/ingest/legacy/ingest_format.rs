use krabka_domain_macros::EnumName;

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumName)]
pub enum IngestFormat {
    #[name(value = "pprof")]
    Pprof,
    #[name(value = "jfr")]
    Jfr,
    #[name(value = "trie")]
    Trie,
    #[name(value = "tree")]
    Tree,
    #[name(value = "lines")]
    Lines,
    #[name(value = "speedscope")]
    Speedscope,
    /// Anything the parser does not recognise falls back to `Groups`, and
    /// `groups` is the spelling a Pyroscope client sends for it.
    #[name(value = "groups")]
    Groups,
}
