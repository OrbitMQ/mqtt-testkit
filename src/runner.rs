// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Runs cases: each on its own thread, with panic capture, a per-case
//! timeout and bounded parallelism.

use std::cell::RefCell;
use std::panic::{self, AssertUnwindSafe, PanicHookInfo};
use std::sync::mpsc;
use std::sync::{Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::outcome;
use crate::registry::Case;

/// How a case ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Status {
    /// Every check held.
    Passed,
    /// A check failed (or the harness hit an error).
    Failed,
    /// The case did not apply to this broker (see the message).
    Skipped,
    /// The case did not finish within the case timeout.
    TimedOut,
}

impl Status {
    /// Lower-case label: `passed`, `failed`, `skipped`, `timeout`.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::TimedOut => "timeout",
        }
    }

    /// Whether this status counts against conformance.
    #[must_use]
    pub const fn is_failure(self) -> bool {
        matches!(self, Self::Failed | Self::TimedOut)
    }
}

/// The result of one case.
#[derive(Debug, Clone)]
pub struct CaseResult {
    /// The case.
    pub case: &'static Case,
    /// How it ended.
    pub status: Status,
    /// Failure message, or skip reason.
    pub message: Option<String>,
    /// Observations recorded with [`outcome::note`].
    pub notes: Vec<String>,
    /// Wall-clock time.
    pub duration: Duration,
}

/// Runner settings.
#[derive(Debug, Clone)]
pub struct Options {
    /// Cases run at the same time (at least 1).
    pub jobs: usize,
    /// A case running longer than this is reported as [`Status::TimedOut`]
    /// and abandoned.
    pub case_timeout: Duration,
    /// Stop starting cases after the first failure.
    pub fail_fast: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            jobs: 4,
            case_timeout: Duration::from_secs(120),
            fail_fast: false,
        }
    }
}

/// Progress callbacks.
pub enum Event<'a> {
    /// A case started.
    Started(&'static Case),
    /// A case ended.
    Finished(&'a CaseResult),
}

thread_local! {
    static PANIC_MESSAGE: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Prefix of the names of threads that run cases.
const CASE_THREAD: &str = "case:";

/// Installs (once) a panic hook that captures panics on case threads
/// instead of printing them; other threads keep the previous hook.
fn install_panic_hook() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
            let on_case = thread::current()
                .name()
                .is_some_and(|n| n.starts_with(CASE_THREAD));
            if on_case {
                let payload = info
                    .payload()
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_owned())
                    .or_else(|| info.payload().downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "panic".to_owned());
                let message = match info.location() {
                    Some(l) => format!("{payload}\n  at {}:{}", l.file(), l.line()),
                    None => payload,
                };
                PANIC_MESSAGE.with(|m| *m.borrow_mut() = Some(message));
            } else {
                previous(info);
            }
        }));
    });
}

/// Runs one case on the current thread, catching its panic.
fn execute(case: &'static Case) -> CaseResult {
    outcome::reset();
    PANIC_MESSAGE.with(|m| m.borrow_mut().take());
    let start = Instant::now();
    let result = panic::catch_unwind(AssertUnwindSafe(case.run));
    let duration = start.elapsed();
    let record = outcome::take();
    let (status, message) = match result {
        Err(payload) if payload.is::<outcome::SkipNow>() => (
            Status::Skipped,
            Some(
                record
                    .skipped
                    .clone()
                    .unwrap_or_else(|| "skipped".to_owned()),
            ),
        ),
        Err(_) => (
            Status::Failed,
            Some(
                PANIC_MESSAGE
                    .with(|m| m.borrow_mut().take())
                    .unwrap_or_else(|| "panic".to_owned()),
            ),
        ),
        Ok(()) => match record.skipped {
            Some(reason) => (Status::Skipped, Some(reason)),
            None => (Status::Passed, None),
        },
    };
    CaseResult {
        case,
        status,
        message,
        notes: record.notes,
        duration,
    }
}

/// Runs `cases` and returns their results in the given order. `progress`
/// sees each start and end as they happen.
pub fn run(
    cases: &[&'static Case],
    options: &Options,
    mut progress: impl FnMut(Event<'_>),
) -> Vec<CaseResult> {
    install_panic_hook();
    outcome::set_echo(false);
    let jobs = options.jobs.max(1);
    let (tx, rx) = mpsc::channel::<(usize, CaseResult)>();
    let mut results: Vec<Option<CaseResult>> = vec![None; cases.len()];
    let mut running: Vec<(usize, Instant)> = Vec::new();
    let mut next = 0;
    let mut stop = false;

    loop {
        while !stop && running.len() < jobs && next < cases.len() {
            let Some(&case) = cases.get(next) else { break };
            let index = next;
            next += 1;
            progress(Event::Started(case));
            let tx = tx.clone();
            let spawned = thread::Builder::new()
                .name(format!("{CASE_THREAD}{}", case.id))
                .stack_size(8 * 1024 * 1024)
                .spawn(move || {
                    let _ = tx.send((index, execute(case)));
                });
            match spawned {
                Ok(_) => running.push((index, Instant::now())),
                Err(e) => {
                    let r = CaseResult {
                        case,
                        status: Status::Failed,
                        message: Some(format!("cannot start case thread: {e}")),
                        notes: Vec::new(),
                        duration: Duration::ZERO,
                    };
                    progress(Event::Finished(&r));
                    if let Some(slot) = results.get_mut(index) {
                        *slot = Some(r);
                    }
                }
            }
        }
        if running.is_empty() {
            break;
        }
        let now = Instant::now();
        let wait = running
            .iter()
            .map(|(_, started)| (*started + options.case_timeout).saturating_duration_since(now))
            .min()
            .unwrap_or(Duration::ZERO);
        match rx.recv_timeout(wait) {
            Ok((index, result)) => {
                // A result for an abandoned (timed out) case is ignored.
                if let Some(pos) = running.iter().position(|(i, _)| *i == index) {
                    running.swap_remove(pos);
                    if result.status.is_failure() && options.fail_fast {
                        stop = true;
                    }
                    progress(Event::Finished(&result));
                    if let Some(slot) = results.get_mut(index) {
                        *slot = Some(result);
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {
                let now = Instant::now();
                let (expired, alive): (Vec<_>, Vec<_>) = running
                    .into_iter()
                    .partition(|(_, started)| now.duration_since(*started) >= options.case_timeout);
                running = alive;
                for (index, started) in expired {
                    let Some(&case) = cases.get(index) else {
                        continue;
                    };
                    let r = CaseResult {
                        case,
                        status: Status::TimedOut,
                        message: Some(format!("did not finish within {:?}", options.case_timeout)),
                        notes: Vec::new(),
                        duration: now.duration_since(started),
                    };
                    if options.fail_fast {
                        stop = true;
                    }
                    progress(Event::Finished(&r));
                    if let Some(slot) = results.get_mut(index) {
                        *slot = Some(r);
                    }
                }
            }
        }
    }
    outcome::set_echo(true);
    results.into_iter().flatten().collect()
}

/// Default parallelism: the number of CPUs, at most 8.
#[must_use]
pub fn default_jobs() -> usize {
    static JOBS: OnceLock<usize> = OnceLock::new();
    *JOBS.get_or_init(|| thread::available_parallelism().map_or(4, |n| n.get().min(8)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Version;

    fn passes() {}
    fn fails() {
        assert_eq!(1 + 1, 3, "arithmetic");
    }
    fn skips() {
        outcome::note("looked");
        outcome::skipped("not offered");
    }
    fn hangs() {
        thread::sleep(Duration::from_secs(5));
    }

    const fn case(id: &'static str, run: fn()) -> Case {
        Case {
            id,
            version: Version::V5,
            area: "unit",
            name: id,
            doc: "",
            reqs: &[],
            strict: false,
            file: file!(),
            line: 0,
            run,
        }
    }

    static PASS: Case = case("pass", passes);
    static FAIL: Case = case("fail", fails);
    static SKIP: Case = case("skip", skips);
    static HANG: Case = case("hang", hangs);

    #[test]
    fn statuses_messages_and_order() {
        let options = Options {
            jobs: 3,
            case_timeout: Duration::from_millis(500),
            fail_fast: false,
        };
        let mut started = 0;
        let mut finished = 0;
        let results = run(&[&HANG, &PASS, &FAIL, &SKIP], &options, |e| match e {
            Event::Started(_) => started += 1,
            Event::Finished(_) => finished += 1,
        });
        assert_eq!((started, finished), (4, 4));
        let got: Vec<(&str, Status)> = results.iter().map(|r| (r.case.id, r.status)).collect();
        assert_eq!(
            got,
            [
                ("hang", Status::TimedOut),
                ("pass", Status::Passed),
                ("fail", Status::Failed),
                ("skip", Status::Skipped),
            ]
        );
        let fail = &results[2];
        let msg = fail.message.as_deref().unwrap();
        assert!(
            msg.contains("arithmetic") && msg.contains("runner.rs"),
            "{msg}"
        );
        assert_eq!(results[3].message.as_deref(), Some("not offered"));
        assert_eq!(results[3].notes, ["looked"]);
    }

    #[test]
    fn fail_fast_stops_starting_cases() {
        let options = Options {
            jobs: 1,
            case_timeout: Duration::from_secs(5),
            fail_fast: true,
        };
        let results = run(&[&FAIL, &PASS, &PASS], &options, |_| {});
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, Status::Failed);
    }
}
