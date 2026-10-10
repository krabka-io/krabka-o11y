//! Stopping a child process the way an orchestrator stops a container: a
//! `SIGTERM`, then a bounded wait for the process to exit of its own accord.
//!
//! The `SIGTERM` suites of this crate, `//crates/metrics-service` and
//! `//crates/profiles` reach this file with `#[path]`, so it depends only on
//! `assert2`.

use std::{
    process::{Child, Command, ExitStatus},
    time::{Duration, Instant},
};

/// Sends `child` a `SIGTERM` and waits up to `within` for it to exit.
///
/// # Panics
///
/// Panics when the signal cannot be sent, or when the child is still running
/// once `within` has passed; the child is killed first, so no process
/// outlives the test.
pub fn terminate_and_wait_for_exit(child: &mut Child, within: Duration) -> ExitStatus {
    // Through `sh` rather than a `kill` binary: the shell builtin is always
    // there, including inside a Bazel test sandbox, and `unsafe_code` is
    // forbidden workspace-wide so `libc::kill` is not an option.
    let signalled = Command::new("/bin/sh")
        .arg("-c")
        .arg(format!("kill -TERM {}", child.id()))
        .status()
        .expect("send SIGTERM");
    assert2::assert!(signalled.success());

    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        match child.try_wait().expect("poll the child") {
            Some(status) => return status,
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("the child process did not exit within {within:?} of SIGTERM");
}
