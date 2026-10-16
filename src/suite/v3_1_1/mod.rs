// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! MQTT 3.1.1 conformance: CONNECT/CONNACK, malformed packets, keep-alive,
//! PINGREQ, wills, PUBLISH/SUBSCRIBE flows, retained messages, topics and
//! persistent sessions.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    reason = "conformance cases: a failed unwrap or assertion is a test failure"
)]

pub(crate) mod connect;
pub(crate) mod interop;
pub(crate) mod lifecycle;
pub(crate) mod malformed;
pub(crate) mod persistence;
pub(crate) mod pubsub;
pub(crate) mod retain;
pub(crate) mod topics;
pub(crate) mod websocket;
pub(crate) mod will;

use crate::{Broker, Client, Connect, Version, default_timeout, unique_id};

const V: Version = Version::V311;

/// A connected 3.1.1 client with a fresh clean session.
fn session(broker: &Broker) -> Client {
    broker
        .connect_ok(&Connect::new(V, &unique_id("s311")))
        .unwrap()
        .0
}

/// Connects, sends `bytes` and requires the broker to close the connection
/// without sending anything back.
fn assert_violation_closes(broker: &Broker, bytes: &[u8]) {
    let mut c = session(broker);
    c.send_raw(bytes).unwrap();
    let packets = c.expect_closed(default_timeout()).unwrap();
    assert!(
        packets.is_empty(),
        "broker answered {bytes:02x?} with {packets:?} instead of closing"
    );
}

/// Sends `connect` on a fresh connection and requires a close with no CONNACK.
fn assert_connect_closed_without_connack(broker: &Broker, connect: &Connect) {
    let mut c = broker.open(V).unwrap();
    c.send(&crate::Packet::Connect(Box::new(connect.clone())))
        .unwrap();
    let packets = c.expect_closed(default_timeout()).unwrap();
    assert!(
        packets.is_empty(),
        "invalid CONNECT {connect:?} answered with {packets:?}"
    );
}

/// A topic (or filter prefix) unique to this call.
fn topic() -> String {
    format!("conformance/{}", unique_id("t"))
}

/// Connects `client_id` with CleanSession=0 and returns the client and the
/// CONNACK's Session Present flag.
fn persistent(broker: &Broker, client_id: &str) -> (Client, bool) {
    let (c, ack) = broker
        .connect_ok(&Connect::new(V, client_id).clean(false))
        .unwrap();
    (c, ack.session_present())
}
