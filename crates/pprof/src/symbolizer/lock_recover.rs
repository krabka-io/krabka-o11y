use super::{Mutex, MutexGuard};

/// Recover a poisoned mutex rather than propagate the panic.
///
/// One panicked worker must not permanently `DoS` the resolver. This function
/// takes ownership of the inner guard and continues.
pub(crate) fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
