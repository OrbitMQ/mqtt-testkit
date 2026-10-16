// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Locating or spawning the broker under test.

use std::ffi::OsStr;
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use crate::client::Client;
use crate::codec::{ConnAck, Connect, Version};
use crate::{HarnessError, env_millis};

/// The MQTT-over-WebSocket endpoint (`ws://host:port/path`): set with
/// [`set_ws_endpoint`] (the CLI's `--ws`), else `MQTT_TESTKIT_WS`. A broker
/// spawned per case has no discoverable WebSocket port, so WebSocket cases
/// use only this endpoint.
pub use crate::ws::{endpoint as ws_endpoint, set_endpoint as set_ws_endpoint};

/// Message printed by tests that skip because no broker is configured.
pub const SKIP_MESSAGE: &str = "skipped: no broker (set MQTT_TESTKIT_BIN or MQTT_TESTKIT_BROKER)";

/// Skip reason recorded when no broker is configured.
pub const NO_BROKER: &str = "no broker (set MQTT_TESTKIT_BIN or MQTT_TESTKIT_BROKER)";

/// Where the broker under test comes from, when chosen programmatically
/// (by the CLI) rather than through the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// An already running broker at `host:port`.
    Address(String),
    /// Spawn `program` with `args` (whitespace-separated, `{addr}` and
    /// `{port}` substituted) on a free loopback port for every case.
    Spawn {
        /// Executable.
        program: String,
        /// Arguments.
        args: String,
    },
}

/// The programmatic target; overrides `MQTT_TESTKIT_BIN` / `MQTT_TESTKIT_BROKER`.
static TARGET: RwLock<Option<Target>> = RwLock::new(None);

/// Process ids of brokers spawned and not yet reaped.
static SPAWNED: Mutex<Vec<u32>> = Mutex::new(Vec::new());

/// Sets (or with `None` clears) the broker target for this process. It takes
/// precedence over the environment variables.
pub fn set_target(target: Option<Target>) {
    *TARGET.write().unwrap_or_else(PoisonError::into_inner) = target;
}

/// The current programmatic target, if any.
#[must_use]
pub fn target() -> Option<Target> {
    TARGET
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
}

/// Kills every spawned broker that is still running (e.g. one held by a case
/// abandoned after its timeout). Called by the runner before exiting.
pub fn kill_spawned() {
    let pids = std::mem::take(&mut *SPAWNED.lock().unwrap_or_else(PoisonError::into_inner));
    for pid in pids {
        let _ = Command::new("kill")
            .args(["-9", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// How many times [`Broker::spawn`] retries on a fresh port.
pub const SPAWN_ATTEMPTS: usize = 8;

/// Serialises port allocation and broker startup within this process.
static SPAWN_LOCK: Mutex<()> = Mutex::new(());

/// The broker under test. A spawned broker is killed when this is dropped.
#[derive(Debug)]
pub struct Broker {
    addr: String,
    child: Option<Child>,
}

impl Broker {
    /// Resolves the broker from the environment:
    ///
    /// 1. `MQTT_TESTKIT_BIN`: spawn that binary with `ORBITMQ_LISTEN=127.0.0.1:<free port>`
    ///    (plus the optional whitespace-separated `MQTT_TESTKIT_BIN_ARGS`, in which
    ///    `{addr}` and `{port}` are substituted), and wait until it accepts.
    /// 2. `MQTT_TESTKIT_BROKER=host:port`: use an already running broker.
    /// 3. Neither: `Ok(None)`, and the caller skips.
    pub fn from_env() -> Result<Option<Self>, HarnessError> {
        Self::from_env_with(&[])
    }

    /// Like [`Broker::from_env`], but a broker spawned from `MQTT_TESTKIT_BIN`
    /// also gets the environment variables `env` (e.g. `ORBITMQ_CONFIG` for a
    /// test that needs a specific broker configuration). They cannot be
    /// applied to an already running `MQTT_TESTKIT_BROKER`; check
    /// [`Broker::is_spawned`].
    pub fn from_env_with(env: &[(&str, &OsStr)]) -> Result<Option<Self>, HarnessError> {
        match target() {
            Some(Target::Address(addr)) => return Ok(Some(Self { addr, child: None })),
            Some(Target::Spawn { program, args }) => {
                return Self::spawn_with_env(&program, &args, env).map(Some);
            }
            None => {}
        }
        if let Some(bin) = crate::env_var("MQTT_TESTKIT_BIN") {
            let args = crate::env_var("MQTT_TESTKIT_BIN_ARGS").unwrap_or_default();
            return Self::spawn_with_env(&bin, &args, env).map(Some);
        }
        Ok(crate::env_var("MQTT_TESTKIT_BROKER").map(|addr| Self { addr, child: None }))
    }

    /// Like [`broker_or_skip!`](crate::broker_or_skip) for helpers that
    /// return a value: `None` (after printing [`SKIP_MESSAGE`]) when no
    /// broker is configured.
    ///
    /// # Panics
    ///
    /// If a broker is configured but cannot be started or reached.
    #[must_use]
    #[allow(
        clippy::panic,
        reason = "test helper: an unavailable broker fails the test"
    )]
    pub fn for_test() -> Option<Self> {
        Self::for_test_with(&[])
    }

    /// [`Broker::for_test`] with [`Broker::from_env_with`]'s extra
    /// environment for a spawned broker.
    ///
    /// # Panics
    ///
    /// If a broker is configured but cannot be started or reached.
    #[must_use]
    #[allow(
        clippy::panic,
        reason = "test helper: an unavailable broker fails the test"
    )]
    pub fn for_test_with(env: &[(&str, &OsStr)]) -> Option<Self> {
        match Self::from_env_with(env) {
            Ok(Some(b)) => Some(b),
            Ok(None) => {
                crate::outcome::skipped(NO_BROKER);
                None
            }
            Err(e) => panic!("broker under test unavailable: {e}"),
        }
    }

    /// Spawns `bin` on a free loopback port and waits for it to accept.
    ///
    /// Picking a port by binding port 0 and releasing it is racy: between the
    /// release and the broker's own bind, another process (or another test
    /// in this one) can take the port, and the readiness probe would then
    /// reach a foreign broker. So:
    ///
    /// - allocation, spawn and readiness run under a process-wide lock, so
    ///   tests of one binary never hand the same port to two brokers;
    /// - after readiness the harness checks that the child is still alive and
    ///   (where `lsof` exists) that the child's pid owns the listening socket;
    /// - on any failure the child is killed and the whole sequence is retried
    ///   on a fresh port (up to [`SPAWN_ATTEMPTS`] times).
    ///
    /// `MQTT_TESTKIT_VERIFY_OWNER=0` disables the ownership check (for wrappers
    /// whose listening process is not the spawned pid).
    pub fn spawn(bin: &str, args: &str) -> Result<Self, HarnessError> {
        Self::spawn_with_env(bin, args, &[])
    }

    /// [`Broker::spawn`] with extra environment variables for the child.
    pub fn spawn_with_env(
        bin: &str,
        args: &str,
        env: &[(&str, &OsStr)],
    ) -> Result<Self, HarnessError> {
        let timeout = Duration::from_millis(env_millis("MQTT_TESTKIT_STARTUP_TIMEOUT_MS", 10_000));
        let mut errors = Vec::new();
        for _ in 0..SPAWN_ATTEMPTS {
            match Self::spawn_once(bin, args, env, timeout) {
                Ok(b) => return Ok(b),
                Err(Attempt::Retry(e)) => {
                    eprintln!("conformance: retrying broker spawn on a fresh port: {e}");
                    errors.push(e.to_string());
                }
                Err(Attempt::Fatal(e)) => return Err(e),
            }
        }
        Err(HarnessError::Broker(format!(
            "could not start {bin} after {SPAWN_ATTEMPTS} attempts: {}",
            errors.join("; ")
        )))
    }

    fn spawn_once(
        bin: &str,
        args: &str,
        env: &[(&str, &OsStr)],
        timeout: Duration,
    ) -> Result<Self, Attempt> {
        let _guard = SPAWN_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .map_err(|e| Attempt::Fatal(e.into()))?
            .port();
        let addr = format!("127.0.0.1:{port}");
        let args: Vec<String> = args
            .split_whitespace()
            .map(|a| {
                a.replace("{addr}", &addr)
                    .replace("{port}", &port.to_string())
            })
            .collect();
        let child = Command::new(bin)
            .args(&args)
            .envs(env.iter().copied())
            .env("ORBITMQ_LISTEN", &addr)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| {
                Attempt::Fatal(HarnessError::Broker(format!("cannot spawn {bin}: {e}")))
            })?;
        SPAWNED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(child.id());
        let mut broker = Self {
            addr,
            child: Some(child),
        };
        // On error `broker` is dropped here, which kills the child.
        broker.wait_ready(timeout)?;
        broker.verify_owner(port).map_err(Attempt::Retry)?;
        Ok(broker)
    }

    /// Checks that the process listening on `port` is the spawned child.
    fn verify_owner(&mut self, port: u16) -> Result<(), HarnessError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        let pid = child.id();
        let verify = crate::env_var("MQTT_TESTKIT_VERIFY_OWNER").is_none_or(|v| v.trim() != "0");
        let mut confirmed = false;
        if verify
            && let Ok(out) = Command::new("lsof")
                .args(["-nP", &format!("-iTCP:{port}"), "-sTCP:LISTEN", "-t"])
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .output()
        {
            let pids: Vec<u32> = String::from_utf8_lossy(&out.stdout)
                .split_whitespace()
                .filter_map(|p| p.parse().ok())
                .collect();
            if !pids.is_empty() && !pids.contains(&pid) {
                return Err(HarnessError::Broker(format!(
                    "port {port} is owned by pid(s) {pids:?}, not the spawned broker {pid}"
                )));
            }
            confirmed = pids.contains(&pid);
        }
        if !confirmed {
            // No lsof (or it saw nothing): a broker that lost the bind race
            // exits promptly, so give it a moment before the liveness check.
            std::thread::sleep(Duration::from_millis(50));
        }
        if let Some(status) = child.try_wait()? {
            return Err(HarnessError::Broker(format!(
                "broker exited right after startup: {status}"
            )));
        }
        Ok(())
    }

    fn wait_ready(&mut self, timeout: Duration) -> Result<(), Attempt> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(child) = self.child.as_mut()
                && let Some(status) = child.try_wait().map_err(|e| Attempt::Fatal(e.into()))?
            {
                // Typically "address already in use": retry on another port.
                return Err(Attempt::Retry(HarnessError::Broker(format!(
                    "broker exited during startup: {status}"
                ))));
            }
            if TcpStream::connect(&self.addr).is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Attempt::Fatal(HarnessError::Broker(format!(
                    "broker did not accept on {} within {timeout:?}",
                    self.addr
                ))));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Whether this broker was spawned by the harness (`MQTT_TESTKIT_BIN`) rather
    /// than reached at `MQTT_TESTKIT_BROKER`.
    #[must_use]
    pub fn is_spawned(&self) -> bool {
        self.child.is_some()
    }

    /// `host:port` of the broker.
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Opens a raw TCP connection.
    pub fn open(&self, version: Version) -> Result<Client, HarnessError> {
        Client::open(&self.addr, version)
    }

    /// Opens a connection, sends `connect` and returns the CONNACK (any code).
    pub fn connect(&self, connect: &Connect) -> Result<(Client, ConnAck), HarnessError> {
        let mut c = self.open(connect.version)?;
        let ack = c.connect(connect)?;
        Ok((c, ack))
    }

    /// Like [`Broker::connect`] but requires a successful CONNACK (code 0).
    pub fn connect_ok(&self, connect: &Connect) -> Result<(Client, ConnAck), HarnessError> {
        let (c, ack) = self.connect(connect)?;
        if ack.code == 0 {
            Ok((c, ack))
        } else {
            Err(HarnessError::Unexpected(format!(
                "CONNECT {:?} refused: {ack:?}",
                connect.client_id
            )))
        }
    }
}

/// Outcome of one spawn attempt that did not produce a usable broker.
enum Attempt {
    /// Lost a port race (or similar): try again on a fresh port.
    Retry(HarnessError),
    /// Retrying cannot help.
    Fatal(HarnessError),
}

impl Drop for Broker {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
            let pid = child.id();
            SPAWNED
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .retain(|p| *p != pid);
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn lsof_available() -> bool {
        Command::new("lsof")
            .arg("-v")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    }

    #[test]
    fn a_port_owned_by_another_process_is_detected() {
        if !lsof_available() {
            println!("skipped: lsof not available");
            return;
        }
        // The "foreign broker": a listener owned by this test process.
        let foreign = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = foreign.local_addr().unwrap().port();
        // The "spawned broker": alive, but not listening anywhere.
        let child = Command::new("sleep").arg("30").spawn().unwrap();
        let mut broker = Broker {
            addr: format!("127.0.0.1:{port}"),
            child: Some(child),
        };
        let err = broker.verify_owner(port).unwrap_err();
        assert!(err.to_string().contains("not the spawned broker"), "{err}");
    }

    #[test]
    fn a_child_that_exits_during_startup_is_retried() {
        let err = Broker::spawn("false", "").unwrap_err().to_string();
        assert!(
            err.contains(&format!("after {SPAWN_ATTEMPTS} attempts"))
                && err.contains("exited during startup"),
            "{err}"
        );
    }

    #[test]
    fn a_missing_binary_is_not_retried() {
        let err = Broker::spawn("/nonexistent/orbitmq", "")
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot spawn"), "{err}");
    }
}
