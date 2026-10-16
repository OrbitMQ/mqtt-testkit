// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Real-client smoke test with the Eclipse Mosquitto command-line clients.
//!
//! When `mosquitto_pub` and `mosquitto_sub` are on `PATH`, publishes and
//! subscribes at QoS 0, 1 and 2, and sets, reads and clears a retained
//! message, for each of `-V mqttv31`, `mqttv311` and `mqttv5`. Skips when no
//! broker is configured or the clients are missing.

use std::io::{BufRead as _, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use crate::{Broker, outcome, unique_id};

/// How long a client may take before the test gives up.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(15);

fn clients_available() -> bool {
    ["mosquitto_pub", "mosquitto_sub"].iter().all(|bin| {
        Command::new(bin)
            .arg("--help")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    })
}

/// Resolves the broker and checks for the clients; `None` means skip.
fn setup() -> Option<(Broker, String, String)> {
    let broker = Broker::for_test()?;
    if !clients_available() {
        outcome::skipped("mosquitto_pub/mosquitto_sub not on PATH");
        return None;
    }
    let (host, port) = broker
        .addr()
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("broker address {:?} has no port", broker.addr()));
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = port.to_owned();
    Some((broker, host, port))
}

/// Kills `child` if it outlives [`CLIENT_TIMEOUT`]; returns its exit status
/// (or `None` on timeout).
fn wait_bounded(child: &mut Child) -> Option<std::process::ExitStatus> {
    let deadline = std::time::Instant::now() + CLIENT_TIMEOUT;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn publish(host: &str, port: &str, version: &str, qos: u8, topic: &str, extra: &[&str]) {
    let mut child = Command::new("mosquitto_pub")
        .args([
            "-h",
            host,
            "-p",
            port,
            "-V",
            version,
            "-q",
            &qos.to_string(),
        ])
        .args(["-t", topic, "-i", &unique_id("mpub")])
        .args(extra)
        .stdin(Stdio::null())
        .spawn()
        .unwrap_or_else(|e| panic!("cannot run mosquitto_pub: {e}"));
    let status = wait_bounded(&mut child);
    assert!(
        status.is_some_and(|s| s.success()),
        "mosquitto_pub -V {version} -q {qos} {extra:?} failed: {status:?}"
    );
}

/// `mosquitto_pub` / `mosquitto_sub -V mqttv31` at QoS 0, 1 and 2.
pub fn v3_1_publish_and_subscribe() {
    publish_and_subscribe("mqttv31");
}

/// `mosquitto_pub` / `mosquitto_sub -V mqttv311` at QoS 0, 1 and 2.
pub fn v3_1_1_publish_and_subscribe() {
    // An independent client (the Mosquitto command-line clients) interoperates
    // with the broker using only spec-defined MQTT 3.1.1 features.
    // req: 3.1.1/MQTT-7.0.0-2
    publish_and_subscribe("mqttv311");
}

/// `mosquitto_pub` / `mosquitto_sub -V mqttv5` at QoS 0, 1 and 2.
pub fn v5_publish_and_subscribe() {
    publish_and_subscribe("mqttv5");
}

/// Set, read and clear a retained message with `-V mqttv31`.
pub fn v3_1_retained_message() {
    retained_message("mqttv31");
}

/// Set, read and clear a retained message with `-V mqttv311`.
pub fn v3_1_1_retained_message() {
    retained_message("mqttv311");
}

/// Set, read and clear a retained message with `-V mqttv5`.
pub fn v5_retained_message() {
    retained_message("mqttv5");
}

fn publish_and_subscribe(version: &str) {
    let Some((_broker, host, port)) = setup() else {
        return;
    };
    {
        for qos in 0..=2u8 {
            let topic = format!("interop/mosq/{}", unique_id("t"));
            let payload = format!("payload-{version}-qos{qos}");
            // The subscription's readiness is not observable through the
            // client's output: with stdout piped, mosquitto_sub block-buffers
            // its `-d` lines until it prints a message or exits. So publish
            // repeatedly until the subscriber reports the message (`-C 1`
            // makes it exit after the first one) or the deadline passes.
            // `-F` tags the delivered message.
            let mut sub = Command::new("mosquitto_sub")
                .args([
                    "-h",
                    &host,
                    "-p",
                    &port,
                    "-V",
                    version,
                    "-q",
                    &qos.to_string(),
                ])
                .args(["-t", &topic, "-i", &unique_id("msub")])
                .args(["-C", "1", "-W", "10", "-F", "MSG qos=%q %p"])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap_or_else(|e| panic!("cannot run mosquitto_sub: {e}"));
            let stdout = sub.stdout.take().unwrap();
            let expected = format!("MSG qos={qos} {payload}");
            let (tx, rx) = mpsc::channel();
            let wanted = expected.clone();
            let reader = std::thread::spawn(move || {
                let mut lines = Vec::new();
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    if line == wanted {
                        let _ = tx.send(());
                    }
                    lines.push(line);
                }
                lines
            });
            let deadline = std::time::Instant::now() + CLIENT_TIMEOUT;
            while std::time::Instant::now() < deadline {
                publish(&host, &port, version, qos, &topic, &["-m", &payload]);
                if rx.recv_timeout(Duration::from_millis(300)).is_ok() {
                    break;
                }
            }
            let status = wait_bounded(&mut sub);
            let lines = reader.join().unwrap();
            assert!(
                lines.iter().any(|l| l == &expected),
                "-V {version} QoS {qos}: expected {expected:?} (exit {status:?}), got {lines:#?}"
            );
        }
    }
}

fn retained_message(version: &str) {
    let Some((_broker, host, port)) = setup() else {
        return;
    };
    {
        let topic = format!("interop/mosq/{}", unique_id("r"));
        let payload = format!("retained-{version}");
        publish(&host, &port, version, 1, &topic, &["-r", "-m", &payload]);
        let read = |what: &str| {
            let out = Command::new("mosquitto_sub")
                .args(["-h", &host, "-p", &port, "-V", version, "-q", "1"])
                .args(["-t", &topic, "-i", &unique_id("mret")])
                .args(["-C", "1", "-W", "3", "-F", "RET retain=%r %p"])
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .output()
                .unwrap_or_else(|e| panic!("cannot run mosquitto_sub ({what}): {e}"));
            String::from_utf8_lossy(&out.stdout).trim().to_owned()
        };
        assert_eq!(
            read("retained"),
            format!("RET retain=1 {payload}"),
            "-V {version}: retained message not delivered with RETAIN=1"
        );
        // A zero-length retained message clears it; a new subscriber then
        // gets nothing before the -W timeout.
        publish(&host, &port, version, 1, &topic, &["-r", "-n"]);
        assert_eq!(
            read("cleared"),
            "",
            "-V {version}: retained message not cleared"
        );
    }
}
