// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Per-case side channel: skips and notes recorded by the running case.
//!
//! A case that cannot run against this broker (a capability is not offered,
//! no broker is configured) calls [`skipped`] and returns. Informational
//! observations go through [`note`]. Both are recorded for the thread that
//! runs the case and are also printed, so `cargo test -- --nocapture` shows
//! them as before.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};

/// Whether [`skipped`] and [`note`] also print (on for `cargo test`, off
/// while the CLI runner reports them itself).
static ECHO: AtomicBool = AtomicBool::new(true);

/// Turns printing of skips and notes on or off.
pub fn set_echo(on: bool) {
    ECHO.store(on, Ordering::Relaxed);
}

thread_local! {
    static RECORD: RefCell<Record> = RefCell::new(Record::default());
}

/// What a case recorded besides passing or panicking.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Record {
    /// Why the case did not run to completion, if it skipped.
    pub skipped: Option<String>,
    /// Observations worth reporting.
    pub notes: Vec<String>,
}

/// Records that the current case is skipped. The case must return right
/// after calling this; anything it asserts afterwards is still checked.
pub fn skipped(reason: impl Into<String>) {
    let reason = reason.into();
    if ECHO.load(Ordering::Relaxed) {
        println!("skipped: {reason}");
    }
    RECORD.with(|r| {
        r.borrow_mut().skipped.get_or_insert(reason);
    });
}

/// Records an observation about the broker that is not a failure.
pub fn note(text: impl Into<String>) {
    let text = text.into();
    if ECHO.load(Ordering::Relaxed) {
        println!("note: {text}");
    }
    RECORD.with(|r| r.borrow_mut().notes.push(text));
}

/// Clears this thread's record (before a case starts).
pub fn reset() {
    RECORD.with(|r| *r.borrow_mut() = Record::default());
}

/// Takes this thread's record (after a case ends).
#[must_use]
pub fn take() -> Record {
    RECORD.with(|r| std::mem::take(&mut *r.borrow_mut()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_skip_reason_wins_and_notes_accumulate() {
        reset();
        skipped("a");
        skipped("b");
        note("n1");
        note("n2");
        let r = take();
        assert_eq!(r.skipped.as_deref(), Some("a"));
        assert_eq!(r.notes, ["n1", "n2"]);
        assert_eq!(take(), Record::default());
    }
}
