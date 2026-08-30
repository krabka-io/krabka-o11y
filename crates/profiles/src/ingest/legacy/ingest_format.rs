use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestFormat {
    Pprof,
    Jfr,
    Trie,
    Tree,
    Lines,
    Speedscope,
    Groups,
}
