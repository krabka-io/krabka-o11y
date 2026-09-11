use super::ObjectStoreError;

/// Whether trying this object-store failure again could plausibly succeed.
///
/// Only [`ObjectStoreError::Generic`] is transient, and that is not the
/// hedge it looks like. It is the variant the `object_store` HTTP clients
/// produce for *every* failure that is not a status code with a meaning of its
/// own: a connection reset, a request timeout, and a 5xx that survived the
/// client's own retry budget all arrive here. The statuses that do have a
/// meaning are mapped to their own variants on the way out -- 401 to
/// [`Unauthenticated`], 403 to [`PermissionDenied`], 404 to [`NotFound`], 409
/// to [`AlreadyExists`], 412 to [`Precondition`] -- and none of them will read
/// differently in two seconds' time. Retrying them spends the budget that the
/// next transient failure needs and delays the report of the real fault.
///
/// Everything else is permanent, a variant this build has never heard of
/// included: `Error` is `#[non_exhaustive]`, and guessing that an unknown
/// failure is worth another go is the guess that turns into a spin.
///
/// # How this relates to [`BlockReadFailure::skip_reason`]
///
/// The two classifiers answer different questions and agree where they
/// overlap. `skip_reason` asks *whose fault is this block*: a `NotFound` is
/// that one object's problem and a scan may leave it out, while anything else
/// says nothing about the block and must fail the query. This asks *would
/// another attempt help*.
///
/// They agree that `NotFound` is not a store outage. They part company on the
/// rest of the backend errors, which `skip_reason` lumps together as "not this
/// block's fault, propagate": here a 500 and a 403 are opposites, because one
/// is worth waiting out and the other never will be. That split is the whole
/// point of this function, and it is finer than the logs compactor's
/// `block_store_error_is_object_store`, which retries *any* object-store
/// error, a revoked credential included.
///
/// [`Unauthenticated`]: ObjectStoreError::Unauthenticated
/// [`PermissionDenied`]: ObjectStoreError::PermissionDenied
/// [`NotFound`]: ObjectStoreError::NotFound
/// [`AlreadyExists`]: ObjectStoreError::AlreadyExists
/// [`Precondition`]: ObjectStoreError::Precondition
/// [`BlockReadFailure::skip_reason`]: crate::BlockReadFailure::skip_reason
#[must_use]
pub fn is_transient_object_store_error(error: &ObjectStoreError) -> bool {
    matches!(error, ObjectStoreError::Generic { .. })
}
