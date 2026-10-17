// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! MQTT 5.0 conformance: CONNECT/CONNACK, malformed packets and protocol
//! errors, keep-alive, PINGREQ, DISCONNECT, wills, PUBLISH/SUBSCRIBE flows,
//! retained messages, topics, sessions, subscription options, flow control,
//! topic aliases, properties and shared subscriptions.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    reason = "conformance cases: a failed unwrap or assertion is a test failure"
)]

pub(crate) mod connect;
pub(crate) mod flow;
pub(crate) mod interop;
pub(crate) mod lifecycle;
pub(crate) mod malformed;
pub(crate) mod options;
pub(crate) mod persistence;
pub(crate) mod properties;
pub(crate) mod pubsub;
pub(crate) mod retain;
pub(crate) mod shared;
pub(crate) mod topics;
pub(crate) mod websocket;
pub(crate) mod will;

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use crate::{
    Broker, Client, ConnAck, Connect, Packet, PropValue, Property, Version, default_timeout,
    find_prop, prop, unique_id,
};

const V: Version = Version::V5;

/// The Connect Reason Codes defined by the 5.0 CONNACK table (3.2.2.2).
const CONNACK_CODES: [u8; 22] = [
    0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x90, 0x95, 0x97,
    0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9F,
];

/// A connected 5.0 client with a fresh clean session, plus its CONNACK.
fn session_with_ack(broker: &Broker) -> (Client, ConnAck) {
    broker
        .connect_ok(&Connect::new(V, &unique_id("s5")))
        .unwrap()
}

/// A connected 5.0 client with a fresh clean session.
fn session(broker: &Broker) -> Client {
    session_with_ack(broker).0
}

/// Connects, sends `bytes` and requires the broker to close the connection,
/// optionally after a DISCONNECT with an error reason code. Returns that code.
fn assert_error_closes(broker: &Broker, bytes: &[u8]) -> Option<u8> {
    let mut c = session(broker);
    c.send_raw(bytes).unwrap();
    c.expect_error_close(default_timeout())
        .unwrap_or_else(|e| panic!("after {bytes:02x?}: {e}"))
}

/// Sends `connect` on a fresh connection and requires it to be refused: a
/// close, optionally after a CONNACK. For an `MQTT` level 5 CONNECT that
/// CONNACK must carry a defined error code (0x80 or above). Any other
/// CONNECT may be answered under another protocol's rules (MQTT-3.1.2-1/2):
/// a multi-version server replies to `MQTT` with another level using a 3.x
/// CONNACK 0x01 (or 0x84), and to a foreign protocol name with any non-zero
/// code. Returns that CONNACK.
fn refused(broker: &Broker, connect: &Connect) -> Option<ConnAck> {
    let mut c = broker.open(V).unwrap();
    c.send(&Packet::Connect(Box::new(connect.clone()))).unwrap();
    let ack = c
        .expect_refused(default_timeout())
        .unwrap_or_else(|e| panic!("CONNECT {connect:?}: {e}"));
    if let Some(a) = &ack {
        assert!(!a.session_present(), "refusal with Session Present {a:?}");
        if connect.protocol_name == "MQTT" && connect.protocol_level == 5 {
            assert!(a.code >= 0x80, "refusal with non-error code {a:?}");
            assert!(
                CONNACK_CODES.contains(&a.code),
                "undefined CONNACK code {a:?}"
            );
        } else if connect.protocol_name == "MQTT" {
            assert!(matches!(a.code, 0x84 | 0x01), "{a:?}");
        } else {
            assert_ne!(a.code, 0, "{a:?}");
        }
    }
    ack
}

/// A CONNECT with an Authentication Method no server supports.
fn unknown_auth_method_connect() -> Connect {
    Connect::new(V, &unique_id("auth")).property(
        prop::AUTHENTICATION_METHOD,
        PropValue::Str("x-mqtt-testkit-unknown".into()),
    )
}

/// Whether the broker refuses [`unknown_auth_method_connect`] as it must
/// (MQTT-4.12.0-1). Cases that need that refusal as a precondition skip it
/// when it is accepted: `unsupported_authentication_method_is_refused`
/// already reports the violation.
fn refuses_unknown_auth_method(broker: &Broker) -> bool {
    let mut c = broker.open(V).unwrap();
    c.send(&Packet::Connect(Box::new(unknown_auth_method_connect())))
        .unwrap();
    let accepted = matches!(
        c.next_event(default_timeout()),
        Ok(crate::Event::Packet(Packet::ConnAck(ConnAck {
            code: 0,
            ..
        })))
    );
    if accepted {
        crate::outcome::note(
            "unknown Authentication Method accepted (see \
             5.0/connect/unsupported_authentication_method_is_refused)",
        );
    }
    !accepted
}

/// The SUBACK failure Reason Codes defined by the 5.0 table (3.9.3).
const SUBACK_FAILURE_CODES: [u8; 9] = [0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2];

/// A topic (or filter prefix) unique to this call.
fn topic() -> String {
    format!("conformance/{}", unique_id("t"))
}

/// Session Expiry Interval used for persistent sessions in these tests.
const SESSION_EXPIRY: u32 = 300;

/// Connects `client_id` with Clean Start=0 and a Session Expiry Interval of
/// [`SESSION_EXPIRY`]; returns the client and the Session Present flag.
fn persistent(broker: &Broker, client_id: &str) -> (Client, bool) {
    let (c, ack) = broker
        .connect_ok(&Connect::new(V, client_id).clean(false).property(
            prop::SESSION_EXPIRY_INTERVAL,
            PropValue::FourByte(SESSION_EXPIRY),
        ))
        .unwrap();
    (c, ack.session_present())
}

/// Ends the session of `client_id` (Clean Start=1, expiry 0, DISCONNECT).
fn forget(broker: &Broker, client_id: &str) {
    let (c, _) = broker.connect_ok(&Connect::new(V, client_id)).unwrap();
    c.disconnect().unwrap();
}

/// A numeric CONNACK property, or `default` when absent.
fn connack_u32(ack: &ConnAck, id: u8, default: u32) -> u32 {
    match find_prop(&ack.properties, id) {
        None => default,
        Some(PropValue::Byte(v)) => u32::from(*v),
        Some(PropValue::TwoByte(v)) => u32::from(*v),
        Some(PropValue::FourByte(v) | PropValue::VarInt(v)) => *v,
        Some(other) => panic!("CONNACK property {id:#04x} has type {other:?}"),
    }
}

/// All values of property `id`, in order.
fn props_of(props: &[Property], id: u8) -> Vec<PropValue> {
    props
        .iter()
        .filter(|p| p.id == id)
        .map(|p| p.value.clone())
        .collect()
}

/// How the broker rejected a SUBSCRIBE.
#[derive(Debug, PartialEq, Eq)]
enum Rejection {
    /// SUBACK with this failure reason code (>= 0x80).
    SubAck(u8),
    /// Connection closed, after a DISCONNECT with this reason code if any.
    Closed(Option<u8>),
}

/// Sends a SUBSCRIBE for `filter` with `options` and requires a rejection:
/// either a SUBACK with a failure code (>= 0x80) or an error close (a
/// DISCONNECT, if any, must carry a code >= 0x80). Both are allowed by the
/// spec for an invalid filter (failure code, or Protocol Error / Malformed
/// Packet handling).
fn subscribe_rejected(c: &mut Client, filter: &str, options: u8) -> Rejection {
    let packet_id = c.next_packet_id();
    c.send(&Packet::Subscribe(crate::Subscribe {
        packet_id,
        properties: Vec::new(),
        filters: vec![(filter.to_owned(), options)],
    }))
    .unwrap();
    match c.next_event(default_timeout()).unwrap() {
        crate::Event::Packet(Packet::SubAck(s)) => {
            assert_eq!(s.packet_id, packet_id);
            assert_eq!(s.codes.len(), 1, "{s:?}");
            assert!(s.codes[0] >= 0x80, "filter {filter:?} accepted: {s:?}");
            assert!(
                SUBACK_FAILURE_CODES.contains(&s.codes[0]),
                "filter {filter:?}: undefined SUBACK code {s:?}"
            );
            Rejection::SubAck(s.codes[0])
        }
        crate::Event::Packet(Packet::Disconnect(d)) => {
            let reason = d.reason.unwrap_or(0);
            assert!(reason >= 0x80, "filter {filter:?}: DISCONNECT {d:?}");
            c.expect_closed(default_timeout()).unwrap();
            Rejection::Closed(Some(reason))
        }
        crate::Event::Closed => Rejection::Closed(None),
        other => panic!("filter {filter:?}: expected a rejection, got {other:?}"),
    }
}

/// What [`Raw::next`] read.
#[derive(Debug)]
enum RawEvent {
    /// A packet, with its exact bytes on the wire.
    Frame(Packet, Vec<u8>),
    /// The broker closed the connection.
    Closed,
    /// Nothing arrived in time.
    Timeout,
}

/// A 5.0 connection that keeps the exact bytes of every packet the broker
/// sends, for checks on wire layout and packet sizes that a re-encoded
/// [`Packet`] cannot answer (e.g. an empty Property Length that a decoder
/// treats like an absent one).
struct Raw {
    stream: TcpStream,
    buf: Vec<u8>,
    closed: bool,
}

impl Raw {
    /// Opens a TCP connection and sends `connect` (the CONNACK is left
    /// unread).
    fn connect(broker: &Broker, connect: &Connect) -> Self {
        let stream = TcpStream::connect(broker.addr()).unwrap();
        stream.set_nodelay(true).unwrap();
        stream.set_write_timeout(Some(default_timeout())).unwrap();
        let mut raw = Self {
            stream,
            buf: Vec::new(),
            closed: false,
        };
        raw.send(&Packet::Connect(Box::new(connect.clone())));
        raw
    }

    fn send(&mut self, packet: &Packet) {
        let bytes = packet.encode(V);
        self.stream.write_all(&bytes).unwrap();
    }

    /// The next packet with its bytes, a close, or a timeout.
    fn next(&mut self, timeout: Duration) -> RawEvent {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some((p, n)) = crate::codec::decode(&self.buf, V).unwrap() {
                let bytes: Vec<u8> = self.buf.drain(..n).collect();
                return RawEvent::Frame(p, bytes);
            }
            if self.closed {
                assert!(
                    self.buf.is_empty(),
                    "close inside a packet: {:02x?}",
                    self.buf
                );
                return RawEvent::Closed;
            }
            let now = Instant::now();
            if now >= deadline {
                return RawEvent::Timeout;
            }
            self.stream
                .set_read_timeout(Some((deadline - now).max(Duration::from_millis(1))))
                .unwrap();
            let mut tmp = [0u8; 4096];
            match self.stream.read(&mut tmp) {
                Ok(0) => self.closed = true,
                Ok(n) => self.buf.extend_from_slice(&tmp[..n]),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => self.closed = true,
            }
        }
    }

    /// The next packet and its bytes; anything else fails the case.
    fn frame(&mut self) -> (Packet, Vec<u8>) {
        match self.next(default_timeout()) {
            RawEvent::Frame(p, b) => (p, b),
            other => panic!("expected a packet, got {other:?}"),
        }
    }
}

/// Splits a frame into its first byte and its body (after the Remaining
/// Length), checking that the Remaining Length matches.
fn frame_body(bytes: &[u8]) -> (u8, &[u8]) {
    let (len, n) = crate::codec::decode_varint(&bytes[1..]).unwrap().unwrap();
    let body = &bytes[1 + n..];
    assert_eq!(body.len(), len as usize, "{bytes:02x?}");
    (bytes[0], body)
}

/// Reads a Property Length at the start of `body` and returns the bytes
/// after the properties. Fails if the field is missing or overruns.
fn skip_properties(body: &[u8]) -> &[u8] {
    let (len, n) = crate::codec::decode_varint(body)
        .unwrap()
        .unwrap_or_else(|| panic!("no Property Length in {body:02x?}"));
    body.get(n + len as usize..)
        .unwrap_or_else(|| panic!("Property Length {len} overruns {body:02x?}"))
}
