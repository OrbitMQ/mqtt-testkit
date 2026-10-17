// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! MQTT 3.1 (`MQIsdp`, level 3) conformance: CONNECT/CONNACK, malformed
//! packets, keep-alive, PINGREQ and will handling.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    reason = "conformance cases: a failed unwrap or assertion is a test failure"
)]

pub(crate) mod connect;
pub(crate) mod lifecycle;
pub(crate) mod malformed;
pub(crate) mod persistence;
pub(crate) mod pubsub;
pub(crate) mod retain;
pub(crate) mod topics;
pub(crate) mod will;

use crate::{Broker, Client, Connect, Packet, Version, default_timeout, unique_id};

const V: Version = Version::V31;

/// A connected 3.1 client with a fresh clean session.
fn session(broker: &Broker) -> Client {
    broker
        .connect_ok(&Connect::new(V, &unique_id("s31")))
        .unwrap()
        .0
}

/// Connects, sends `bytes` and requires the broker to close the connection
/// without answering (a DISCONNECT before the close is noted, see
/// [`crate::Client::expect_disconnect`]).
fn assert_violation_closes(broker: &Broker, bytes: &[u8]) {
    let mut c = session(broker);
    c.send_raw(bytes).unwrap();
    c.expect_disconnect(default_timeout())
        .unwrap_or_else(|e| panic!("after {bytes:02x?}: {e}"));
}

/// Sends `connect` on a fresh connection and returns the CONNACK of a
/// refusal (`None` if the broker closed without one). 3.1 never says the
/// server closes after a non-zero CONNACK (that is 3.1.1 MQTT-3.2.2-5), so a
/// connection left open after one is only noted.
fn refused(broker: &Broker, connect: &Connect) -> Option<crate::ConnAck> {
    let mut c = broker.open(V).unwrap();
    c.send(&Packet::Connect(Box::new(connect.clone()))).unwrap();
    let (packets, closed) = c
        .packets_until_close(default_timeout())
        .unwrap_or_else(|e| panic!("CONNECT {connect:?}: {e}"));
    match packets.as_slice() {
        [] if closed => None,
        [Packet::ConnAck(a)] if a.code != 0 => {
            if !closed {
                crate::outcome::note(format!(
                    "CONNACK {:#04x}; the connection was left for the client to close",
                    a.code
                ));
            }
            Some(a.clone())
        }
        [] => panic!("CONNECT {connect:?}: neither refused nor closed"),
        other => panic!("CONNECT {connect:?}: expected a refusal, got {other:?}"),
    }
}

/// A topic (or filter prefix) unique to this call.
fn topic() -> String {
    format!("conformance/{}", unique_id("t"))
}

/// Connects `client_id` with CleanSession=0.
fn persistent(broker: &Broker, client_id: &str) -> Client {
    broker
        .connect_ok(&Connect::new(V, client_id).clean(false))
        .unwrap()
        .0
}
