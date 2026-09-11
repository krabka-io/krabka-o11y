//! Where a panic is one request's problem, and where it is the role's.
//!
//! The two halves of this milestone pull in opposite directions, and the
//! boundary between them is a listener.
//!
//! **Inside a request, contain it.** A handler that panics on one malformed
//! body has found a bug in that code path, not a reason to drop the
//! connection and leave the client guessing. Without containment the panic
//! unwinds through hyper's per-connection task and the socket closes with no
//! status at all, which a client reports as a transport error and a dashboard
//! reports as nothing. [`contain_handler_panics`] turns it into a 500, so the
//! request fails the way every other failure does and the connection survives
//! for the next one.
//!
//! **Inside a background loop, do not.** A WAL consumer or an index refresher
//! that panics leaves a role that still listens, still answers, and answers
//! from a tier that stopped advancing. There is nothing to return a 500 to.
//! That case belongs to
//! [`SupervisedTasks`](crate::SupervisedTasks), which cancels the role and
//! lets the process exit.
//!
//! Containment is not repair. A caught panic leaves whatever the handler was
//! part-way through exactly as it was, so shared state on the request path has
//! to be written such that a panic cannot leave it half-updated -- build the
//! new value privately and publish it in one move -- or else stay visibly
//! broken. A `std::sync` lock poisons on an unwind while it is held
//! exclusively, and that poison is the signal; clearing it without the
//! publish-on-success discipline behind it converts a loud fault into a silent
//! one, which is the failure mode this milestone exists to remove.

use crate::{IntoResponse, Response, Router, StatusCode};

mod catch_panic;
mod contain_handler_panics;
mod panic_message;
mod panic_safe_shared;
#[cfg(test)]
mod tests;

pub(crate) use catch_panic::catch_panic;
pub use contain_handler_panics::contain_handler_panics;
pub(crate) use panic_message::panic_message;
pub use panic_safe_shared::PanicSafeShared;
