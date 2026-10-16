// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Requirement-mapped, black-box MQTT conformance and interoperability
//! suite for any MQTT 3.1 / 3.1.1 / 5.0 broker.
//!
//! Cases drive the broker under test over raw TCP using this crate's own
//! encoder/decoder ([`codec`]), which shares nothing with any broker.
//! Each case carries `// req: <version>/<ID>` markers naming the normative
//! statements it checks; [`spec`] holds the requirement catalogue and
//! [`registry`] the cases. [`runner`] executes them and [`report`] renders
//! the results; the `mqtt-testkit` binary is the command-line front end.
//!
//! The broker is located by [`Broker::from_env`] (the CLI sets it with
//! [`broker::set_target`]); with no broker configured, [`broker_or_skip!`]
//! records a skip.
//!
//! All fallible functions return [`HarnessError`].
#![allow(clippy::missing_errors_doc)]

pub mod broker;
pub mod client;
pub mod codec;
pub mod interop;
pub mod outcome;
pub mod registry;
pub mod report;
pub mod runner;
pub mod spec;
mod suite;
pub mod ws;

use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use broker::{Broker, NO_BROKER, SKIP_MESSAGE, Target};
pub use client::{Client, Event};
pub use codec::{
    Ack, ConnAck, Connect, Packet, PropValue, Property, Publish, ReasonOnly, SubAck, Subscribe,
    Unsubscribe, Version, Will, Writer, find_prop, frame, prop,
};
pub use ws::WebSocket;

/// Harness failure.
#[derive(Debug)]
pub enum HarnessError {
    /// Socket error.
    Io(std::io::Error),
    /// The broker sent bytes the harness codec rejects.
    Decode(codec::CodecError),
    /// Nothing (or not enough) happened before the deadline.
    Timeout(String),
    /// The broker closed the connection when that was not expected.
    Closed(String),
    /// The broker sent something other than what was expected.
    Unexpected(String),
    /// The broker under test could not be started or reached.
    Broker(String),
}

impl fmt::Display for HarnessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Decode(e) => write!(f, "{e}"),
            Self::Timeout(m) => write!(f, "timeout: {m}"),
            Self::Closed(m) => write!(f, "connection closed: {m}"),
            Self::Unexpected(m) => write!(f, "unexpected: {m}"),
            Self::Broker(m) => write!(f, "broker: {m}"),
        }
    }
}

impl std::error::Error for HarnessError {}

impl From<std::io::Error> for HarnessError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<codec::CodecError> for HarnessError {
    fn from(e: codec::CodecError) -> Self {
        Self::Decode(e)
    }
}

/// Environment variables, as `(name, legacy name)`. The legacy `ORBITMQ_*`
/// names from before the rename are still read when the new name is unset.
pub const ENV_VARS: &[(&str, &str)] = &[
    ("MQTT_TESTKIT_BIN_ARGS", "ORBITMQ_BIN_ARGS"),
    ("MQTT_TESTKIT_BIN", "ORBITMQ_BIN"),
    ("MQTT_TESTKIT_BROKER", "ORBITMQ_TEST_BROKER"),
    ("MQTT_TESTKIT_WS", "ORBITMQ_TEST_WS"),
    (
        "MQTT_TESTKIT_STARTUP_TIMEOUT_MS",
        "ORBITMQ_STARTUP_TIMEOUT_MS",
    ),
    ("MQTT_TESTKIT_VERIFY_OWNER", "ORBITMQ_VERIFY_OWNER"),
    ("MQTT_TESTKIT_TIMEOUT_MS", "ORBITMQ_TEST_TIMEOUT_MS"),
    ("MQTT_TESTKIT_QUIET_MS", "ORBITMQ_TEST_QUIET_MS"),
    (
        "MQTT_TESTKIT_INTEROP_DENY_FILTER",
        "ORBITMQ_INTEROP_DENY_FILTER",
    ),
    (
        "MQTT_TESTKIT_INTEROP_SERVER_KEEP_ALIVE",
        "ORBITMQ_INTEROP_SERVER_KEEP_ALIVE",
    ),
];

/// The value of environment variable `name` (an `MQTT_TESTKIT_*` name), or
/// of its legacy `ORBITMQ_*` alias when `name` is unset or empty.
#[must_use]
pub fn env_var(name: &str) -> Option<String> {
    let legacy = ENV_VARS.iter().find(|(n, _)| *n == name).map(|(_, l)| *l);
    std::iter::once(name)
        .chain(legacy)
        .find_map(|n| std::env::var(n).ok().filter(|v| !v.is_empty()))
}

/// Reads a millisecond value from the environment.
#[must_use]
pub fn env_millis(name: &str, default: u64) -> u64 {
    env_var(name)
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// Programmatic overrides (milliseconds, 0 = none) of the two waits below.
static TIMEOUT_OVERRIDE: AtomicU64 = AtomicU64::new(0);
static QUIET_OVERRIDE: AtomicU64 = AtomicU64::new(0);

/// Overrides [`default_timeout`] and [`quiet_period`] for this process (the
/// CLI's `--timeout-ms` / `--quiet-ms`). `None` keeps the current value.
pub fn set_waits(timeout: Option<Duration>, quiet: Option<Duration>) {
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX).max(1);
    if let Some(t) = timeout {
        TIMEOUT_OVERRIDE.store(ms(t), Ordering::Relaxed);
    }
    if let Some(q) = quiet {
        QUIET_OVERRIDE.store(ms(q), Ordering::Relaxed);
    }
}

fn wait(over: &AtomicU64, env: &str, default: u64) -> Duration {
    match over.load(Ordering::Relaxed) {
        0 => Duration::from_millis(env_millis(env, default)),
        ms => Duration::from_millis(ms),
    }
}

/// Default wait for an expected broker reaction: 2 s, or
/// `MQTT_TESTKIT_TIMEOUT_MS`.
#[must_use]
pub fn default_timeout() -> Duration {
    wait(&TIMEOUT_OVERRIDE, "MQTT_TESTKIT_TIMEOUT_MS", 2_000)
}

/// How long tests wait to conclude that something did *not* happen
/// (e.g. a will was not published): 1 s, or `MQTT_TESTKIT_QUIET_MS`.
#[must_use]
pub fn quiet_period() -> Duration {
    wait(&QUIET_OVERRIDE, "MQTT_TESTKIT_QUIET_MS", 1_000)
}

/// A client identifier (or topic segment) unique to this process and call:
/// `prefix` followed by hex digits, at most 23 ASCII alphanumerics.
#[must_use]
pub fn unique_id(prefix: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let mut id: String = prefix
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(8)
        .collect();
    let _ = write!(
        id,
        "{:x}{:x}{n:x}",
        std::process::id() & 0xFFFF,
        nanos & 0xFF_FFFF
    );
    id.truncate(23);
    id
}

/// Resolves the broker under test, or prints [`SKIP_MESSAGE`] and returns
/// from the calling test. Panics if a configured broker cannot be started.
#[macro_export]
macro_rules! broker_or_skip {
    () => {
        match $crate::Broker::from_env() {
            Ok(Some(broker)) => broker,
            Ok(None) => {
                $crate::outcome::skipped($crate::NO_BROKER);
                return;
            }
            Err(e) => panic!("broker under test unavailable: {e}"),
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_ids_are_valid_client_ids() {
        let a = unique_id("conformance");
        let b = unique_id("conformance");
        assert_ne!(a, b);
        for id in [a, b] {
            assert!(!id.is_empty() && id.len() <= 23, "{id}");
            assert!(id.chars().all(|c| c.is_ascii_alphanumeric()), "{id}");
        }
    }

    #[test]
    fn every_env_var_has_the_new_prefix_and_a_legacy_alias() {
        for (name, legacy) in ENV_VARS {
            assert!(name.starts_with("MQTT_TESTKIT_"), "{name}");
            assert!(legacy.starts_with("ORBITMQ_"), "{legacy}");
        }
        assert_eq!(env_var("MQTT_TESTKIT_NO_SUCH_VARIABLE"), None);
    }

    #[test]
    fn default_timeout_is_two_seconds_unless_overridden() {
        if env_var("MQTT_TESTKIT_TIMEOUT_MS").is_none()
            && TIMEOUT_OVERRIDE.load(Ordering::Relaxed) == 0
        {
            assert_eq!(default_timeout(), Duration::from_secs(2));
        }
    }
}
