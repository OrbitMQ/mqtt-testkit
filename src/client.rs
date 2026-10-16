// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Raw-socket test client.

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use crate::codec::{
    self, Ack, ConnAck, Connect, Packet, Property, Publish, ReasonOnly, SubAck, Subscribe,
    Unsubscribe, Version,
};
use crate::ws::{OP_BINARY, OP_CLOSE, WebSocket, WsEvent};
use crate::{HarnessError, default_timeout};

/// The connection under a [`Client`]: raw TCP, or MQTT over WebSocket
/// (every MQTT byte sent travels in one binary frame per send).
#[derive(Debug)]
enum Transport {
    Tcp(TcpStream),
    Ws(Box<WebSocket>),
}

/// What the next read from the broker produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// A complete packet.
    Packet(Packet),
    /// The broker closed (or reset) the connection.
    Closed,
    /// Nothing arrived before the timeout.
    Timeout,
}

/// A TCP (or WebSocket) connection to the broker under test that speaks
/// MQTT through the harness's own codec and can also send arbitrary bytes.
#[derive(Debug)]
pub struct Client {
    transport: Transport,
    version: Version,
    buf: Vec<u8>,
    closed: bool,
    next_id: u16,
    /// PUBLISH / PUBREL packets that arrived while a helper waited for
    /// something else (e.g. a SUBACK); [`Client::next_event`] returns them
    /// first, in arrival order.
    stash: VecDeque<Packet>,
}

impl Client {
    /// Opens a TCP connection without sending anything.
    pub fn open(addr: &str, version: Version) -> Result<Self, HarnessError> {
        let timeout = default_timeout();
        let mut last = None;
        for sa in addr.to_socket_addrs()? {
            match TcpStream::connect_timeout(&sa, timeout) {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    stream.set_write_timeout(Some(timeout))?;
                    return Ok(Self::with_transport(Transport::Tcp(stream), version));
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.map_or_else(
            || HarnessError::Broker(format!("{addr} did not resolve")),
            HarnessError::Io,
        ))
    }

    /// Opens MQTT over WebSocket at `url` (`ws://host:port/path`), offering
    /// the `mqtt` subprotocol, without sending any MQTT packet. Fails if the
    /// upgrade is refused or invalid (see [`WebSocket::connect`]); which
    /// subprotocol the server selected is left to the caller to check.
    pub fn open_ws(url: &str, version: Version) -> Result<Self, HarnessError> {
        Ok(Self::from_websocket(
            WebSocket::connect(url, &["mqtt"])?,
            version,
        ))
    }

    /// Wraps an already upgraded WebSocket.
    #[must_use]
    pub fn from_websocket(ws: WebSocket, version: Version) -> Self {
        Self::with_transport(Transport::Ws(Box::new(ws)), version)
    }

    fn with_transport(transport: Transport, version: Version) -> Self {
        Self {
            transport,
            version,
            buf: Vec::new(),
            closed: false,
            next_id: 0,
            stash: VecDeque::new(),
        }
    }

    /// Whether this client runs over WebSocket.
    #[must_use]
    pub const fn is_websocket(&self) -> bool {
        matches!(self.transport, Transport::Ws(_))
    }

    /// The WebSocket under this client (for raw frames, the handshake or
    /// the server's close code), if it runs over WebSocket.
    pub fn websocket(&mut self) -> Option<&mut WebSocket> {
        match &mut self.transport {
            Transport::Ws(ws) => Some(ws),
            Transport::Tcp(_) => None,
        }
    }

    /// Protocol version used to decode broker packets.
    #[must_use]
    pub const fn version(&self) -> Version {
        self.version
    }

    /// Changes the decoding version (e.g. after a raw CONNECT).
    pub const fn set_version(&mut self, version: Version) {
        self.version = version;
    }

    /// Returns a fresh non-zero packet identifier.
    pub fn next_packet_id(&mut self) -> u16 {
        self.next_id = self.next_id.checked_add(1).unwrap_or(1);
        self.next_id
    }

    /// Sends raw bytes (over WebSocket: as one binary frame).
    pub fn send_raw(&mut self, bytes: &[u8]) -> Result<(), HarnessError> {
        let stream = match &mut self.transport {
            Transport::Tcp(stream) => stream,
            Transport::Ws(ws) => {
                let r = ws.send_binary(bytes);
                if matches!(r, Err(HarnessError::Closed(_))) {
                    self.closed = true;
                }
                return r;
            }
        };
        match stream.write_all(bytes).and_then(|()| stream.flush()) {
            Ok(()) => Ok(()),
            Err(e) if is_closed(e.kind()) => {
                self.closed = true;
                Err(HarnessError::Closed(format!("while sending: {e}")))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Encodes and sends a packet with this client's version.
    pub fn send(&mut self, packet: &Packet) -> Result<(), HarnessError> {
        let bytes = packet.encode(self.version);
        self.send_raw(&bytes)
    }

    /// Sends CONNECT and waits for CONNACK (whatever its code).
    pub fn connect(&mut self, connect: &Connect) -> Result<ConnAck, HarnessError> {
        self.version = connect.version;
        self.send(&Packet::Connect(Box::new(connect.clone())))?;
        match self.expect_packet(default_timeout())? {
            Packet::ConnAck(c) => Ok(c),
            p => Err(HarnessError::Unexpected(format!(
                "expected CONNACK, got {p:?}"
            ))),
        }
    }

    /// Reads the next event (packet, close or timeout).
    pub fn next_event(&mut self, timeout: Duration) -> Result<Event, HarnessError> {
        if let Some(p) = self.stash.pop_front() {
            return Ok(Event::Packet(p));
        }
        let deadline = Instant::now() + timeout;
        loop {
            if let Some((p, n)) = codec::decode(&self.buf, self.version)? {
                self.buf.drain(..n);
                return Ok(Event::Packet(p));
            }
            if self.closed {
                if self.buf.is_empty() {
                    return Ok(Event::Closed);
                }
                return Err(HarnessError::Unexpected(format!(
                    "connection closed with {} bytes of a partial packet",
                    self.buf.len()
                )));
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(Event::Timeout);
            }
            let stream = match &mut self.transport {
                Transport::Tcp(stream) => stream,
                Transport::Ws(ws) => {
                    match ws.next_message(deadline - now)? {
                        WsEvent::Message { opcode, payload } if opcode == OP_BINARY => {
                            self.buf.extend_from_slice(&payload);
                        }
                        WsEvent::Message { opcode, payload } => {
                            return Err(HarnessError::Unexpected(format!(
                                "broker sent MQTT data in a non-binary WebSocket frame \
                                 (opcode {opcode:#x}, {} bytes)",
                                payload.len()
                            )));
                        }
                        WsEvent::Closed => self.closed = true,
                        WsEvent::Timeout => {}
                    }
                    continue;
                }
            };
            stream.set_read_timeout(Some((deadline - now).max(Duration::from_millis(1))))?;
            let mut tmp = [0u8; 4096];
            match stream.read(&mut tmp) {
                Ok(0) => self.closed = true,
                Ok(n) => self.buf.extend_from_slice(tmp.get(..n).unwrap_or(&[])),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) if is_closed(e.kind()) => self.closed = true,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Waits for the next packet.
    pub fn expect_packet(&mut self, timeout: Duration) -> Result<Packet, HarnessError> {
        match self.next_event(timeout)? {
            Event::Packet(p) => Ok(p),
            Event::Closed => Err(HarnessError::Closed(
                "expected a packet but the broker closed the connection".into(),
            )),
            Event::Timeout => Err(HarnessError::Timeout(format!(
                "no packet within {timeout:?}"
            ))),
        }
    }

    /// Waits until the broker closes the connection, returning every packet
    /// received before the close. Fails if the connection is still open.
    pub fn expect_closed(&mut self, timeout: Duration) -> Result<Vec<Packet>, HarnessError> {
        let deadline = Instant::now() + timeout;
        let mut seen = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.next_event(left)? {
                Event::Packet(p) => seen.push(p),
                Event::Closed => return Ok(seen),
                Event::Timeout => {
                    return Err(HarnessError::Timeout(format!(
                        "connection still open after {timeout:?} (received {:?})",
                        seen.iter().map(Packet::name).collect::<Vec<_>>()
                    )));
                }
            }
        }
    }

    /// Collects what the broker sends within `timeout`, stopping early at a
    /// close. Returns the packets and whether the connection was closed;
    /// unlike [`Client::expect_closed`], a connection left open is not an
    /// error.
    pub fn packets_until_close(
        &mut self,
        timeout: Duration,
    ) -> Result<(Vec<Packet>, bool), HarnessError> {
        let deadline = Instant::now() + timeout;
        let mut seen = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.next_event(left)? {
                Event::Packet(p) => seen.push(p),
                Event::Closed => return Ok((seen, true)),
                Event::Timeout => return Ok((seen, false)),
            }
        }
    }

    /// Like [`Client::expect_closed`], for a close the spec requires without
    /// a deadline: waits up to 30 s and notes a close that came after
    /// [`default_timeout`] (for example when a server ignores a packet and
    /// closes at its own idle or CONNECT timeout).
    pub fn expect_closed_eventually(&mut self) -> Result<Vec<Packet>, HarnessError> {
        let started = Instant::now();
        let packets = self.expect_closed(Duration::from_secs(30))?;
        let took = started.elapsed();
        if took > default_timeout() {
            crate::outcome::note(format!("connection closed after {took:.1?}"));
        }
        Ok(packets)
    }

    /// Waits for the broker to end the connection, allowing (in 5.0) a
    /// DISCONNECT first. Returns its reason code, `None` for a bare close.
    /// In 3.x, where a server never sends DISCONNECT, a 5.0-style one before
    /// the close is noted and otherwise treated as a bare close: it is not an
    /// answer to the client's packet. Any other packet is an error.
    pub fn expect_disconnect(&mut self, timeout: Duration) -> Result<Option<u8>, HarnessError> {
        let packets = self.expect_closed(timeout)?;
        match packets.as_slice() {
            [] => Ok(None),
            [Packet::Disconnect(d)] if self.version.is_v5() => Ok(Some(d.reason.unwrap_or(0))),
            [Packet::Disconnect(d)] => {
                self.note_v3_disconnect(d.reason);
                Ok(None)
            }
            other => Err(HarnessError::Unexpected(format!(
                "expected only a close (or a 5.0 DISCONNECT), got {other:?}"
            ))),
        }
    }

    /// Like [`Client::next_event`], except that in 3.x a DISCONNECT followed
    /// by the close is noted and reported as [`Event::Closed`] (see
    /// [`Client::expect_disconnect`]).
    pub fn next_event_or_close(&mut self, timeout: Duration) -> Result<Event, HarnessError> {
        match self.next_event(timeout)? {
            Event::Packet(Packet::Disconnect(d)) if !self.version.is_v5() => {
                match self.next_event(default_timeout())? {
                    Event::Closed => {
                        self.note_v3_disconnect(d.reason);
                        Ok(Event::Closed)
                    }
                    other => Err(HarnessError::Unexpected(format!(
                        "DISCONNECT {d:?}, then {other:?} instead of a close"
                    ))),
                }
            }
            e => Ok(e),
        }
    }

    fn note_v3_disconnect(&self, reason: Option<u8>) {
        let reason = reason.map_or_else(|| "none".to_owned(), |r| format!("{r:#04x}"));
        crate::outcome::note(format!(
            "server sent an MQTT {} client a DISCONNECT (reason {reason}) before closing; \
             3.x defines none from the server",
            self.version
        ));
    }

    /// Waits for the broker to close the connection because of an error the
    /// client caused. In 3.x the close must not be preceded by any packet; in
    /// 5.0 a DISCONNECT is allowed and its reason code must be 0x80 or above.
    /// Returns the DISCONNECT reason code, if any.
    pub fn expect_error_close(&mut self, timeout: Duration) -> Result<Option<u8>, HarnessError> {
        let reason = self.expect_disconnect(timeout)?;
        match reason {
            Some(r) if r < 0x80 => Err(HarnessError::Unexpected(format!(
                "DISCONNECT for an error carried success reason code {r:#04x}"
            ))),
            other => Ok(other),
        }
    }

    /// Waits for the broker to refuse a CONNECT: either a CONNACK with a
    /// non-zero code followed by a close, or a close without CONNACK.
    /// Returns the CONNACK if one was sent. A successful CONNACK, any other
    /// packet, or a connection left open is an error.
    pub fn expect_refused(&mut self, timeout: Duration) -> Result<Option<ConnAck>, HarnessError> {
        let packets = self.expect_closed(timeout)?;
        match packets.as_slice() {
            [] => Ok(None),
            [Packet::ConnAck(c)] if c.code != 0 => Ok(Some(c.clone())),
            other => Err(HarnessError::Unexpected(format!(
                "expected a refused CONNECT, got {other:?}"
            ))),
        }
    }

    /// Asserts that no packet arrives (and the connection stays open) for
    /// `duration`.
    pub fn expect_no_packet(&mut self, duration: Duration) -> Result<(), HarnessError> {
        match self.next_event(duration)? {
            Event::Timeout => Ok(()),
            Event::Packet(p) => Err(HarnessError::Unexpected(format!(
                "expected silence, got {p:?}"
            ))),
            Event::Closed => Err(HarnessError::Closed(
                "expected silence but the broker closed the connection".into(),
            )),
        }
    }

    /// Sends PINGREQ and waits for PINGRESP.
    pub fn ping(&mut self) -> Result<(), HarnessError> {
        self.send(&Packet::PingReq)?;
        match self.expect_packet(default_timeout())? {
            Packet::PingResp => Ok(()),
            p => Err(HarnessError::Unexpected(format!(
                "expected PINGRESP, got {p:?}"
            ))),
        }
    }

    /// Waits for a packet accepted by `want`, stashing any PUBLISH or PUBREL
    /// that arrives first (see [`Client::next_event`]). Any other packet, a
    /// close or a timeout is an error.
    pub fn wait_for<T>(
        &mut self,
        timeout: Duration,
        what: &str,
        mut want: impl FnMut(&Packet) -> Option<T>,
    ) -> Result<T, HarnessError> {
        let deadline = Instant::now() + timeout;
        let mut stashed = VecDeque::new();
        let result = loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let p = match self.next_event(left) {
                Ok(Event::Packet(p)) => p,
                Ok(Event::Closed) => {
                    break Err(HarnessError::Closed(format!(
                        "while waiting for {what} (received {:?})",
                        stashed.iter().map(Packet::name).collect::<Vec<_>>()
                    )));
                }
                Ok(Event::Timeout) => {
                    break Err(HarnessError::Timeout(format!(
                        "no {what} within {timeout:?}"
                    )));
                }
                Err(e) => break Err(e),
            };
            if let Some(t) = want(&p) {
                break Ok(t);
            }
            if matches!(p, Packet::Publish(_) | Packet::PubRel(_)) {
                stashed.push_back(p);
            } else {
                break Err(HarnessError::Unexpected(format!(
                    "expected {what}, got {p:?}"
                )));
            }
        };
        // Earlier stash entries were re-read first by `next_event`, so
        // `stashed` already holds everything in arrival order.
        self.stash.extend(stashed);
        result
    }

    /// Subscribes to one filter and waits for the SUBACK.
    pub fn subscribe(&mut self, filter: &str, options: u8) -> Result<SubAck, HarnessError> {
        self.subscribe_many(&[(filter, options)], Vec::new())
    }

    /// Sends one SUBSCRIBE with several filters (and 5.0 properties) and
    /// waits for its SUBACK.
    pub fn subscribe_many(
        &mut self,
        filters: &[(&str, u8)],
        properties: Vec<Property>,
    ) -> Result<SubAck, HarnessError> {
        let packet_id = self.next_packet_id();
        self.send(&Packet::Subscribe(Subscribe {
            packet_id,
            properties,
            filters: filters.iter().map(|(f, o)| ((*f).to_owned(), *o)).collect(),
        }))?;
        self.wait_for(default_timeout(), "SUBACK", |p| match p {
            Packet::SubAck(s) if s.packet_id == packet_id => Some(s.clone()),
            _ => None,
        })
    }

    /// Unsubscribes from one filter and waits for the UNSUBACK.
    pub fn unsubscribe(&mut self, filter: &str) -> Result<SubAck, HarnessError> {
        self.unsubscribe_many(&[filter])
    }

    /// Sends one UNSUBSCRIBE with several filters and waits for the UNSUBACK.
    pub fn unsubscribe_many(&mut self, filters: &[&str]) -> Result<SubAck, HarnessError> {
        let packet_id = self.next_packet_id();
        self.send(&Packet::Unsubscribe(Unsubscribe {
            packet_id,
            properties: Vec::new(),
            filters: filters.iter().map(|f| (*f).to_owned()).collect(),
        }))?;
        self.wait_for(default_timeout(), "UNSUBACK", |p| match p {
            Packet::UnsubAck(s) if s.packet_id == packet_id => Some(s.clone()),
            _ => None,
        })
    }

    /// Publishes at QoS 0, 1 or 2 and completes the acknowledgement flow.
    pub fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
    ) -> Result<(), HarnessError> {
        let mut p = Publish::new(topic, payload);
        p.qos = qos;
        p.retain = retain;
        self.publish_packet(p).map(|_| ())
    }

    /// Sends `p` (assigning a packet identifier if QoS > 0 and none is set)
    /// and completes its flow. Returns the PUBACK (QoS 1) or PUBREC (QoS 2),
    /// so tests can inspect reason codes. For QoS 2 the PUBREL/PUBCOMP
    /// exchange is completed unless the PUBREC carries a reason code of 0x80
    /// or above.
    pub fn publish_packet(&mut self, mut p: Publish) -> Result<Option<Ack>, HarnessError> {
        if p.qos > 0 && p.packet_id.is_none() {
            p.packet_id = Some(self.next_packet_id());
        }
        let id = p.packet_id.unwrap_or(0);
        let qos = p.qos;
        self.send(&Packet::Publish(p))?;
        match qos {
            0 => Ok(None),
            1 => self
                .wait_for(default_timeout(), "PUBACK", |p| match p {
                    Packet::PubAck(a) if a.packet_id == id => Some(a.clone()),
                    _ => None,
                })
                .map(Some),
            _ => {
                let rec = self.wait_for(default_timeout(), "PUBREC", |p| match p {
                    Packet::PubRec(a) if a.packet_id == id => Some(a.clone()),
                    _ => None,
                })?;
                if rec.reason.unwrap_or(0) < 0x80 {
                    self.send(&Packet::PubRel(Ack::new(id)))?;
                    self.wait_for(default_timeout(), "PUBCOMP", |p| match p {
                        Packet::PubComp(a) if a.packet_id == id => Some(()),
                        _ => None,
                    })?;
                }
                Ok(Some(rec))
            }
        }
    }

    /// Waits for a PUBLISH, acknowledging it if QoS > 0 (PUBACK, or PUBREC
    /// for QoS 2). A PUBREL from the broker that arrives first is answered
    /// with PUBCOMP and skipped.
    pub fn expect_publish(&mut self, timeout: Duration) -> Result<Publish, HarnessError> {
        let p = self.expect_publish_unacked(timeout)?;
        match (p.qos, p.packet_id) {
            (1, Some(id)) => self.send(&Packet::PubAck(Ack::new(id)))?,
            (2, Some(id)) => self.send(&Packet::PubRec(Ack::new(id)))?,
            _ => {}
        }
        Ok(p)
    }

    /// Waits for a PUBLISH without acknowledging it. A PUBREL from the
    /// broker that arrives first is answered with PUBCOMP and skipped.
    pub fn expect_publish_unacked(&mut self, timeout: Duration) -> Result<Publish, HarnessError> {
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.expect_packet(left)? {
                Packet::Publish(p) => return Ok(p),
                Packet::PubRel(a) => self.send(&Packet::PubComp(Ack::new(a.packet_id)))?,
                p => {
                    return Err(HarnessError::Unexpected(format!(
                        "expected PUBLISH, got {p:?}"
                    )));
                }
            }
        }
    }

    /// Waits for up to `n` PUBLISH packets without acknowledging them: the first
    /// within `timeout`, each further one within [`crate::quiet_period`]. A
    /// server may keep fewer messages in flight than the client allows (down
    /// to one at a time), so fewer than `n` is not an error; the caller
    /// adapts, and a note records how many were in flight.
    pub fn expect_in_flight(
        &mut self,
        n: usize,
        timeout: Duration,
    ) -> Result<Vec<Publish>, HarnessError> {
        let mut out = vec![self.expect_publish_unacked(timeout)?];
        while out.len() < n {
            match self.expect_publish_unacked(crate::quiet_period()) {
                Ok(p) => out.push(p),
                Err(HarnessError::Timeout(_)) => {
                    crate::outcome::note(format!(
                        "server kept {} of {n} QoS>0 messages in flight",
                        out.len()
                    ));
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// Collects every PUBLISH that arrives within `window`, acknowledging
    /// each one (and answering PUBRELs). Any other packet or a close is an
    /// error.
    pub fn collect_publishes(&mut self, window: Duration) -> Result<Vec<Publish>, HarnessError> {
        let deadline = Instant::now() + window;
        let mut out = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(out);
            }
            match self.expect_publish(left) {
                Ok(p) => out.push(p),
                Err(HarnessError::Timeout(_)) => return Ok(out),
                Err(e) => return Err(e),
            }
        }
    }

    /// Collects exactly `n` PUBLISH packets (acknowledging them), waiting at
    /// most `timeout` for each.
    pub fn expect_publishes(
        &mut self,
        n: usize,
        timeout: Duration,
    ) -> Result<Vec<Publish>, HarnessError> {
        (0..n).map(|_| self.expect_publish(timeout)).collect()
    }

    /// Checks keep-alive enforcement for an effective keep-alive of
    /// `keep_alive`: PINGREQs sent every 0.6 x `keep_alive` for two periods
    /// keep the connection open; after going quiet the broker must close it
    /// within 1.5 x `keep_alive` (plus [`default_timeout`] of grace for
    /// scheduling). Returns how long the broker took to close after the
    /// last packet was sent.
    pub fn check_keep_alive(&mut self, keep_alive: Duration) -> Result<Duration, HarnessError> {
        let active_until = Instant::now() + keep_alive * 2;
        while Instant::now() < active_until {
            std::thread::sleep(keep_alive.mul_f32(0.6));
            self.ping()?;
        }
        let quiet_since = Instant::now();
        let limit = keep_alive.mul_f32(1.5) + default_timeout();
        let packets = self.expect_closed(limit)?;
        let allowed = match packets.as_slice() {
            [] => true,
            [Packet::Disconnect(d)] => self.version.is_v5() && d.reason.unwrap_or(0) >= 0x80,
            _ => false,
        };
        if !allowed {
            return Err(HarnessError::Unexpected(format!(
                "keep-alive close preceded by {packets:?}"
            )));
        }
        Ok(quiet_since.elapsed())
    }

    /// Sends a normal DISCONNECT (reason 0x00 in 5.0) and closes the socket.
    pub fn disconnect(mut self) -> Result<(), HarnessError> {
        let d = if self.version.is_v5() {
            ReasonOnly {
                reason: Some(0),
                properties: Vec::new(),
            }
        } else {
            ReasonOnly::default()
        };
        self.send(&Packet::Disconnect(d))?;
        match &mut self.transport {
            Transport::Tcp(stream) => {
                let _ = stream.shutdown(Shutdown::Write);
            }
            Transport::Ws(ws) => {
                // Normal closure (1000), then half-close like TCP.
                let _ = ws.send_frame(true, OP_CLOSE, &1000u16.to_be_bytes());
                ws.shutdown(Shutdown::Write);
            }
        }
        Ok(())
    }

    /// Drops the TCP connection without DISCONNECT (simulated network
    /// failure; over WebSocket also without a close frame).
    pub fn abort(self) {
        match &self.transport {
            Transport::Tcp(stream) => {
                let _ = stream.shutdown(Shutdown::Both);
            }
            Transport::Ws(ws) => ws.shutdown(Shutdown::Both),
        }
    }
}

fn is_closed(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::NotConnected
            | ErrorKind::UnexpectedEof
    )
}
