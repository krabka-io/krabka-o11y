use super::{BlockStoreError, Error};

#[derive(Debug, Error)]
pub enum PlanError {
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
}
