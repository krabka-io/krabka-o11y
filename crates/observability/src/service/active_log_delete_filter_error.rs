use super::{BlockStoreError, Error, LogDeleteRequestStoreError, ParseError};

#[derive(Debug, Error)]
pub enum ActiveLogDeleteFilterError {
    #[error(transparent)]
    Store(#[from] LogDeleteRequestStoreError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error("stored delete request query {query:?} failed to parse: {source}")]
    Parse { query: String, source: ParseError },
}
