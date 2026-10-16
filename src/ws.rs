// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Minimal RFC 6455 WebSocket client for MQTT over WebSocket (MQTT 3.1.1
//! and 5.0 section 6).
//!
//! Only `ws://host[:port][/path]` URLs (no TLS). The client performs the
//! HTTP/1.1 upgrade, checks the `101` status and `Sec-WebSocket-Accept`,
//! masks every frame it sends, reassembles fragmented messages and answers
//! pings and close frames. For negative tests it can also send frames of
//! any opcode, unfinished fragments and arbitrary chunkings of a byte
//! stream.
//!
//! The endpoint under test is set with [`set_endpoint`] (the CLI's `--ws`)
//! or `MQTT_TESTKIT_WS=ws://host:port/path`; [`endpoint_or_skip`] records a
//! skip when neither is set.

use std::fmt::Write as _;
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::{HarnessError, default_timeout};

/// Continuation frame.
pub const OP_CONTINUATION: u8 = 0x0;
/// Text data frame.
pub const OP_TEXT: u8 = 0x1;
/// Binary data frame.
pub const OP_BINARY: u8 = 0x2;
/// Connection close.
pub const OP_CLOSE: u8 = 0x8;
/// Ping.
pub const OP_PING: u8 = 0x9;
/// Pong.
pub const OP_PONG: u8 = 0xA;

/// The GUID appended to `Sec-WebSocket-Key` (RFC 6455 section 1.3).
const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Largest upgrade response accepted.
const MAX_HANDSHAKE: usize = 16 * 1024;

/// Skip reason recorded when no WebSocket endpoint is configured.
pub const NO_ENDPOINT: &str = "no WebSocket endpoint (--ws / MQTT_TESTKIT_WS)";

static ENDPOINT: RwLock<Option<String>> = RwLock::new(None);

/// Sets (or with `None` clears) the WebSocket endpoint for this process. It
/// takes precedence over `MQTT_TESTKIT_WS`.
pub fn set_endpoint(url: Option<String>) {
    *ENDPOINT.write().unwrap_or_else(PoisonError::into_inner) = url;
}

/// The WebSocket endpoint under test: the programmatic one, else
/// `MQTT_TESTKIT_WS`.
#[must_use]
pub fn endpoint() -> Option<String> {
    if let Some(url) = ENDPOINT
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
    {
        return Some(url);
    }
    crate::env_var("MQTT_TESTKIT_WS").filter(|u| !u.trim().is_empty())
}

/// The configured endpoint, or `None` after recording a skip with
/// [`NO_ENDPOINT`].
#[must_use]
pub fn endpoint_or_skip() -> Option<String> {
    let url = endpoint();
    if url.is_none() {
        crate::outcome::skipped(NO_ENDPOINT);
    }
    url
}

/// A parsed `ws://` URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsUrl {
    /// Host as written (IPv6 literals keep their brackets).
    pub host: String,
    /// Port (default 80).
    pub port: u16,
    /// Request target, starting with `/`.
    pub path: String,
}

impl WsUrl {
    /// Parses `ws://host[:port][/path]`.
    pub fn parse(url: &str) -> Result<Self, HarnessError> {
        let url = url.trim();
        let bad = |why: &str| HarnessError::Broker(format!("WebSocket URL {url:?}: {why}"));
        if url.starts_with("wss://") {
            return Err(bad("wss:// (TLS) is not supported; use ws://"));
        }
        let Some(rest) = url.strip_prefix("ws://") else {
            return Err(bad("expected ws://host:port/path"));
        };
        let (authority, path) = match rest.find('/') {
            Some(i) => (rest.get(..i).unwrap_or(""), rest.get(i..).unwrap_or("/")),
            None => (rest, "/"),
        };
        if authority.is_empty() {
            return Err(bad("missing host"));
        }
        let (host, port) = if let Some(after) = authority.strip_prefix('[') {
            let (h, tail) = after.split_once(']').ok_or_else(|| bad("unclosed '['"))?;
            let port = match tail.strip_prefix(':') {
                Some(p) => p.parse().map_err(|_| bad("invalid port"))?,
                None if tail.is_empty() => 80,
                None => return Err(bad("garbage after IPv6 literal")),
            };
            (format!("[{h}]"), port)
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h.to_owned(), p.parse().map_err(|_| bad("invalid port"))?),
                None => (authority.to_owned(), 80),
            }
        };
        Ok(Self {
            host,
            port,
            path: path.to_owned(),
        })
    }

    /// `host:port` for connecting.
    #[must_use]
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// The server's upgrade response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    /// HTTP status code.
    pub status: u16,
    /// The status line as received.
    pub status_line: String,
    /// Header fields in order, names as received.
    pub headers: Vec<(String, String)>,
}

impl Handshake {
    /// The first value of header `name` (case-insensitive), trimmed.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.trim())
    }

    /// Every `Sec-WebSocket-Protocol` value the server returned (normally
    /// zero or one).
    #[must_use]
    pub fn subprotocols(&self) -> Vec<String> {
        self.headers
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case("Sec-WebSocket-Protocol"))
            .flat_map(|(_, v)| v.split(','))
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

/// One WebSocket frame as received (payload already unmasked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// FIN bit.
    pub fin: bool,
    /// RSV1..3 bits (in the low three bits).
    pub rsv: u8,
    /// Opcode.
    pub opcode: u8,
    /// Whether the frame was masked.
    pub masked: bool,
    /// Payload.
    pub payload: Vec<u8>,
}

/// Result of reading one frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameRead {
    /// A complete frame.
    Frame(Frame),
    /// The TCP connection ended.
    Eof,
    /// Nothing complete arrived before the timeout.
    Timeout,
}

/// Result of reading one data message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsEvent {
    /// A complete (possibly reassembled) data message.
    Message {
        /// Opcode of the first frame ([`OP_BINARY`] or [`OP_TEXT`]).
        opcode: u8,
        /// The reassembled payload.
        payload: Vec<u8>,
    },
    /// The server sent a close frame, or the TCP connection ended.
    Closed,
    /// Nothing arrived before the timeout.
    Timeout,
}

/// Counters of what the server sent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Binary data messages received.
    pub binary: u64,
    /// Text data messages received.
    pub text: u64,
    /// Pings received (each answered with a pong).
    pub pings: u64,
}

/// A close frame received from the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseFrame {
    /// Status code, `None` for an empty close frame.
    pub code: Option<u16>,
}

/// A WebSocket connection (client side).
#[derive(Debug)]
pub struct WebSocket {
    stream: TcpStream,
    handshake: Handshake,
    /// The `Sec-WebSocket-Key` sent.
    key: String,
    /// Raw bytes received but not yet parsed into frames.
    rx: Vec<u8>,
    /// A fragmented message in progress: (opcode, payload so far).
    partial: Option<(u8, Vec<u8>)>,
    eof: bool,
    /// The server's close frame, once received.
    peer_close: Option<CloseFrame>,
    close_sent: bool,
    stats: Stats,
}

impl WebSocket {
    /// Connects to `url` and performs the upgrade offering `protocols` as
    /// `Sec-WebSocket-Protocol` (none sent when empty). Fails unless the
    /// server answers `101` with the right `Upgrade`, `Connection` and
    /// `Sec-WebSocket-Accept`, or if it selects a subprotocol that was not
    /// offered. It does not require a particular subprotocol: check
    /// [`WebSocket::subprotocol`].
    pub fn connect(url: &str, protocols: &[&str]) -> Result<Self, HarnessError> {
        let ws = Self::connect_any_status(url, protocols)?;
        ws.verify_upgrade(protocols)?;
        Ok(ws)
    }

    /// Like [`WebSocket::connect`] but performs no check on the response
    /// beyond parsing its HTTP head (any status is returned).
    pub fn connect_any_status(url: &str, protocols: &[&str]) -> Result<Self, HarnessError> {
        let url = WsUrl::parse(url)?;
        let stream = open_tcp(&url.socket_addr())?;
        let key = base64(&random_bytes::<16>());
        let mut req = format!(
            "GET {} HTTP/1.1\r\nHost: {}:{}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n",
            url.path, url.host, url.port
        );
        if !protocols.is_empty() {
            let _ = write!(req, "Sec-WebSocket-Protocol: {}\r\n", protocols.join(", "));
        }
        req.push_str("\r\n");
        let mut ws = Self {
            stream,
            handshake: Handshake {
                status: 0,
                status_line: String::new(),
                headers: Vec::new(),
            },
            key,
            rx: Vec::new(),
            partial: None,
            eof: false,
            peer_close: None,
            close_sent: false,
            stats: Stats::default(),
        };
        ws.send_bytes(req.as_bytes())?;
        ws.read_handshake(default_timeout())?;
        Ok(ws)
    }

    fn read_handshake(&mut self, timeout: Duration) -> Result<(), HarnessError> {
        let deadline = Instant::now() + timeout;
        let end = loop {
            if let Some(i) = self.rx.windows(4).position(|w| w == b"\r\n\r\n") {
                break i;
            }
            if self.rx.len() > MAX_HANDSHAKE {
                return Err(HarnessError::Unexpected(format!(
                    "WebSocket upgrade response exceeds {MAX_HANDSHAKE} bytes"
                )));
            }
            if self.eof {
                return Err(HarnessError::Closed(format!(
                    "during the WebSocket upgrade (received {:?})",
                    String::from_utf8_lossy(&self.rx)
                )));
            }
            if !self.fill(deadline)? {
                return Err(HarnessError::Timeout(format!(
                    "no WebSocket upgrade response within {timeout:?}"
                )));
            }
        };
        let head: Vec<u8> = self.rx.drain(..end + 4).collect();
        let head = String::from_utf8_lossy(head.get(..end).unwrap_or(&[])).into_owned();
        let mut lines = head.split("\r\n");
        let status_line = lines.next().unwrap_or("").to_owned();
        let mut parts = status_line.split_whitespace();
        let status = match (parts.next(), parts.next().map(str::parse::<u16>)) {
            (Some(v), Some(Ok(code))) if v.starts_with("HTTP/") => code,
            _ => {
                return Err(HarnessError::Unexpected(format!(
                    "not an HTTP response: {status_line:?}"
                )));
            }
        };
        let headers = lines
            .filter_map(|l| l.split_once(':'))
            .map(|(n, v)| (n.trim().to_owned(), v.trim().to_owned()))
            .collect();
        self.handshake = Handshake {
            status,
            status_line,
            headers,
        };
        Ok(())
    }

    /// Checks a `101` response against RFC 6455 section 4.1 (client
    /// requirements on the server's handshake).
    pub fn verify_upgrade(&self, offered: &[&str]) -> Result<(), HarnessError> {
        let hs = &self.handshake;
        let fail = |why: String| {
            Err(HarnessError::Unexpected(format!(
                "WebSocket upgrade: {why}"
            )))
        };
        if hs.status != 101 {
            return fail(format!("refused: {}", hs.status_line));
        }
        if !hs
            .header("Upgrade")
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
        {
            return fail(format!("bad Upgrade header {:?}", hs.header("Upgrade")));
        }
        if !hs.header("Connection").is_some_and(|v| {
            v.split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("upgrade"))
        }) {
            return fail(format!(
                "bad Connection header {:?}",
                hs.header("Connection")
            ));
        }
        let want = accept_key(&self.key);
        if hs.header("Sec-WebSocket-Accept") != Some(want.as_str()) {
            return fail(format!(
                "Sec-WebSocket-Accept {:?}, expected {want:?}",
                hs.header("Sec-WebSocket-Accept")
            ));
        }
        let selected = hs.subprotocols();
        if selected.len() > 1 {
            return fail(format!("several subprotocols selected: {selected:?}"));
        }
        if let Some(p) = selected.first()
            && !offered.contains(&p.as_str())
        {
            return fail(format!("server selected {p:?}, which was not offered"));
        }
        Ok(())
    }

    /// The server's upgrade response.
    #[must_use]
    pub const fn handshake(&self) -> &Handshake {
        &self.handshake
    }

    /// The subprotocol the server selected, if any.
    #[must_use]
    pub fn subprotocol(&self) -> Option<&str> {
        self.handshake.header("Sec-WebSocket-Protocol")
    }

    /// What the server has sent so far.
    #[must_use]
    pub const fn stats(&self) -> Stats {
        self.stats
    }

    /// The server's close frame, if one arrived.
    #[must_use]
    pub const fn peer_close(&self) -> Option<CloseFrame> {
        self.peer_close
    }

    /// Writes raw bytes to the socket (e.g. a hand-made frame).
    pub fn send_bytes(&mut self, bytes: &[u8]) -> Result<(), HarnessError> {
        match self
            .stream
            .write_all(bytes)
            .and_then(|()| self.stream.flush())
        {
            Ok(()) => Ok(()),
            Err(e) if is_closed(e.kind()) => {
                self.eof = true;
                Err(HarnessError::Closed(format!("while sending: {e}")))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Sends one masked frame with the given FIN bit and opcode (any value,
    /// including reserved ones).
    pub fn send_frame(
        &mut self,
        fin: bool,
        opcode: u8,
        payload: &[u8],
    ) -> Result<(), HarnessError> {
        self.send_bytes(&encode_frame(
            fin,
            0,
            opcode,
            payload,
            Some(random_bytes::<4>()),
        ))
    }

    /// Sends one complete binary message.
    pub fn send_binary(&mut self, payload: &[u8]) -> Result<(), HarnessError> {
        self.send_frame(true, OP_BINARY, payload)
    }

    /// Sends one message of `opcode` fragmented into frames of at most
    /// `chunk` payload bytes (first frame `opcode`, then continuations).
    pub fn send_fragmented(
        &mut self,
        opcode: u8,
        payload: &[u8],
        chunk: usize,
    ) -> Result<(), HarnessError> {
        let chunks: Vec<&[u8]> = payload.chunks(chunk.max(1)).collect();
        if chunks.is_empty() {
            return self.send_frame(true, opcode, &[]);
        }
        let last = chunks.len() - 1;
        for (i, c) in chunks.iter().enumerate() {
            let op = if i == 0 { opcode } else { OP_CONTINUATION };
            self.send_frame(i == last, op, c)?;
        }
        Ok(())
    }

    /// Sends `payload` as several complete binary messages of at most
    /// `chunk` bytes each (an MQTT packet split across messages).
    pub fn send_binary_chunks(&mut self, payload: &[u8], chunk: usize) -> Result<(), HarnessError> {
        for c in payload.chunks(chunk.max(1)) {
            self.send_binary(c)?;
        }
        Ok(())
    }

    /// Sends a close frame with `code` (and no reason).
    pub fn send_close(&mut self, code: u16) -> Result<(), HarnessError> {
        self.close_sent = true;
        self.send_frame(true, OP_CLOSE, &code.to_be_bytes())
    }

    /// Shuts down the TCP connection.
    pub fn shutdown(&self, how: Shutdown) {
        let _ = self.stream.shutdown(how);
    }

    /// Reads more bytes into `rx`. `Ok(false)` on timeout; sets `eof` when
    /// the connection ends.
    fn fill(&mut self, deadline: Instant) -> Result<bool, HarnessError> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            self.stream
                .set_read_timeout(Some((deadline - now).max(Duration::from_millis(1))))?;
            let mut tmp = [0u8; 8192];
            match self.stream.read(&mut tmp) {
                Ok(0) => {
                    self.eof = true;
                    return Ok(true);
                }
                Ok(n) => {
                    self.rx.extend_from_slice(tmp.get(..n).unwrap_or(&[]));
                    return Ok(true);
                }
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) if is_closed(e.kind()) => {
                    self.eof = true;
                    return Ok(true);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Reads the next raw frame (control frames are returned, not handled).
    pub fn next_frame(&mut self, timeout: Duration) -> Result<FrameRead, HarnessError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some((frame, n)) = parse_frame(&self.rx)? {
                self.rx.drain(..n);
                return Ok(FrameRead::Frame(frame));
            }
            if self.eof {
                return Ok(FrameRead::Eof);
            }
            if !self.fill(deadline)? {
                return Ok(FrameRead::Timeout);
            }
        }
    }

    /// Reads the next data message, reassembling fragments, answering pings
    /// with pongs and a close frame with a close frame. A frame a server
    /// must not send (masked, fragmented control frame, stray continuation,
    /// reserved opcode or RSV bits) is an error.
    pub fn next_message(&mut self, timeout: Duration) -> Result<WsEvent, HarnessError> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.peer_close.is_some() {
                return Ok(WsEvent::Closed);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            let f = match self.next_frame(left)? {
                FrameRead::Frame(f) => f,
                FrameRead::Eof => return Ok(WsEvent::Closed),
                FrameRead::Timeout => return Ok(WsEvent::Timeout),
            };
            let bad = |why: &str| {
                Err(HarnessError::Unexpected(format!(
                    "server sent an invalid WebSocket frame ({why}): {f:?}"
                )))
            };
            if f.masked {
                return bad("masked");
            }
            if f.rsv != 0 {
                return bad("RSV bits set without a negotiated extension");
            }
            match f.opcode {
                OP_PING | OP_PONG | OP_CLOSE if !f.fin || f.payload.len() > 125 => {
                    return bad("fragmented or oversized control frame");
                }
                OP_PING => {
                    self.stats.pings += 1;
                    // Best effort: the server may already be closing.
                    let _ = self.send_frame(true, OP_PONG, &f.payload);
                }
                OP_PONG => {}
                OP_CLOSE => {
                    let code = match f.payload.as_slice() {
                        [a, b, ..] => Some(u16::from_be_bytes([*a, *b])),
                        _ => None,
                    };
                    self.peer_close = Some(CloseFrame { code });
                    if !self.close_sent {
                        self.close_sent = true;
                        let echo = code.map(u16::to_be_bytes).unwrap_or_default();
                        let body = if code.is_some() { &echo[..] } else { &[][..] };
                        let _ = self.send_frame(true, OP_CLOSE, body);
                    }
                    return Ok(WsEvent::Closed);
                }
                OP_TEXT | OP_BINARY => {
                    if self.partial.is_some() {
                        return bad("new data frame inside a fragmented message");
                    }
                    if f.fin {
                        return Ok(self.message(f.opcode, f.payload));
                    }
                    self.partial = Some((f.opcode, f.payload));
                }
                OP_CONTINUATION => {
                    let Some((op, mut buf)) = self.partial.take() else {
                        return bad("continuation without a message in progress");
                    };
                    buf.extend_from_slice(&f.payload);
                    if f.fin {
                        return Ok(self.message(op, buf));
                    }
                    self.partial = Some((op, buf));
                }
                _ => return bad("reserved opcode"),
            }
        }
    }

    fn message(&mut self, opcode: u8, payload: Vec<u8>) -> WsEvent {
        if opcode == OP_BINARY {
            self.stats.binary += 1;
        } else {
            self.stats.text += 1;
        }
        WsEvent::Message { opcode, payload }
    }
}

fn open_tcp(addr: &str) -> Result<TcpStream, HarnessError> {
    let timeout = default_timeout();
    let mut last = None;
    for sa in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&sa, timeout) {
            Ok(stream) => {
                stream.set_nodelay(true)?;
                stream.set_write_timeout(Some(timeout))?;
                return Ok(stream);
            }
            Err(e) => last = Some(e),
        }
    }
    Err(last.map_or_else(
        || HarnessError::Broker(format!("{addr} did not resolve")),
        HarnessError::Io,
    ))
}

pub(crate) fn is_closed(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::NotConnected
            | ErrorKind::UnexpectedEof
    )
}

/// Encodes one frame. `mask` is the masking key (clients must mask).
#[must_use]
pub fn encode_frame(
    fin: bool,
    rsv: u8,
    opcode: u8,
    payload: &[u8],
    mask: Option<[u8; 4]>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 14);
    out.push(if fin { 0x80 } else { 0 } | ((rsv & 0x7) << 4) | (opcode & 0x0F));
    let m = if mask.is_some() { 0x80 } else { 0 };
    let len = payload.len();
    if len < 126 {
        out.push(m | u8::try_from(len).unwrap_or(0));
    } else if let Ok(l) = u16::try_from(len) {
        out.push(m | 0x7E);
        out.extend_from_slice(&l.to_be_bytes());
    } else {
        out.push(m | 0x7F);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }
    match mask {
        Some(key) => {
            out.extend_from_slice(&key);
            out.extend(payload.iter().zip(key.iter().cycle()).map(|(b, k)| b ^ k));
        }
        None => out.extend_from_slice(payload),
    }
    out
}

/// Parses one frame from the front of `buf`: `None` if incomplete, else the
/// frame and the bytes it used.
pub fn parse_frame(buf: &[u8]) -> Result<Option<(Frame, usize)>, HarnessError> {
    let (Some(&b0), Some(&b1)) = (buf.first(), buf.get(1)) else {
        return Ok(None);
    };
    let masked = b1 & 0x80 != 0;
    let (len, mut pos): (u64, usize) = match b1 & 0x7F {
        126 => match buf.get(2..4) {
            Some(b) => (u64::from(u16::from_be_bytes([b[0], b[1]])), 4),
            None => return Ok(None),
        },
        127 => match buf.get(2..10).and_then(|b| <[u8; 8]>::try_from(b).ok()) {
            Some(b) => (u64::from_be_bytes(b), 10),
            None => return Ok(None),
        },
        l => (u64::from(l), 2),
    };
    let key = if masked {
        let Some(k) = buf
            .get(pos..pos + 4)
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
        else {
            return Ok(None);
        };
        pos += 4;
        Some(k)
    } else {
        None
    };
    let len = usize::try_from(len)
        .map_err(|_| HarnessError::Unexpected(format!("WebSocket frame of {len} bytes")))?;
    let Some(end) = pos.checked_add(len) else {
        return Err(HarnessError::Unexpected(
            "WebSocket frame length overflows".into(),
        ));
    };
    let Some(body) = buf.get(pos..end) else {
        return Ok(None);
    };
    let payload = match key {
        Some(k) => body
            .iter()
            .zip(k.iter().cycle())
            .map(|(b, k)| b ^ k)
            .collect(),
        None => body.to_vec(),
    };
    Ok(Some((
        Frame {
            fin: b0 & 0x80 != 0,
            rsv: (b0 >> 4) & 0x7,
            opcode: b0 & 0x0F,
            masked,
            payload,
        },
        end,
    )))
}

/// `Sec-WebSocket-Accept` for `key`: base64(SHA-1(key + GUID)).
#[must_use]
pub fn accept_key(key: &str) -> String {
    base64(&sha1(format!("{key}{GUID}").as_bytes()))
}

/// Pseudo-random bytes (not cryptographic; for nonces and masking keys).
fn random_bytes<const N: usize>() -> [u8; N] {
    static STATE: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    // Truncation is fine: only the low bits vary anyway.
    let seed = u64::try_from(nanos & u128::from(u64::MAX)).unwrap_or(0)
        ^ (u64::from(std::process::id()) << 32);
    let mut x = STATE
        .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
        .wrapping_add(seed)
        | 1;
    let mut out = [0u8; N];
    for b in &mut out {
        // xorshift64*
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        *b = (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 56) as u8;
    }
    out
}

/// SHA-1 (FIPS 180-4). Used only for the WebSocket accept key.
#[must_use]
#[allow(
    clippy::many_single_char_names,
    reason = "variable names follow FIPS 180-4"
)]
pub fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for block in msg.as_chunks::<64>().0 {
        let mut w = [0u32; 80];
        for (slot, word) in w.iter_mut().zip(block.as_chunks::<4>().0) {
            *slot = u32::from_be_bytes(*word);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let t = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = t;
        }
        for (hv, v) in h.iter_mut().zip([a, b, c, d, e]) {
            *hv = hv.wrapping_add(v);
        }
    }
    let mut out = [0u8; 20];
    for (chunk, v) in out.as_chunks_mut::<4>().0.iter_mut().zip(h) {
        chunk.copy_from_slice(&v.to_be_bytes());
    }
    out
}

/// Standard base64 with padding (RFC 4648 section 4).
#[must_use]
pub fn base64(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let sym = |i: u32| char::from(ALPHABET[(i & 63) as usize]);
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = |i: usize| u32::from(chunk.get(i).copied().unwrap_or(0));
        let n = (b(0) << 16) | (b(1) << 8) | b(2);
        out.push(sym(n >> 18));
        out.push(sym(n >> 12));
        out.push(if chunk.len() > 1 { sym(n >> 6) } else { '=' });
        out.push(if chunk.len() > 2 { sym(n) } else { '=' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().fold(String::new(), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
    }

    #[test]
    fn sha1_known_vectors() {
        assert_eq!(hex(&sha1(b"")), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            hex(&sha1(b"abc")),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(
            hex(&sha1(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
        assert_eq!(
            hex(&sha1(&vec![b'a'; 1_000_000])),
            "34aa973cd4c4daa4f61eeb2bdbad27316534016f"
        );
    }

    #[test]
    fn base64_known_vectors() {
        for (input, want) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(input.as_bytes()), want, "{input:?}");
        }
    }

    #[test]
    fn accept_key_matches_rfc6455_example() {
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn frames_round_trip_with_and_without_mask() {
        for len in [0usize, 5, 125, 126, 65_535, 65_536] {
            let payload: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
            for mask in [None, Some([1, 2, 3, 4])] {
                let bytes = encode_frame(len % 2 == 0, 0, OP_BINARY, &payload, mask);
                // Incomplete input is not a frame yet.
                assert_eq!(parse_frame(&bytes[..bytes.len() - 1]).unwrap(), None);
                let (f, n) = parse_frame(&bytes).unwrap().unwrap();
                assert_eq!(n, bytes.len());
                assert_eq!(f.payload, payload);
                assert_eq!(f.masked, mask.is_some());
                assert_eq!(f.fin, len % 2 == 0);
                assert_eq!(f.opcode, OP_BINARY);
            }
        }
    }

    #[test]
    fn rfc6455_unmasked_hello_example() {
        let (f, n) = parse_frame(&[0x81, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f])
            .unwrap()
            .unwrap();
        assert_eq!((f.opcode, f.fin, n), (OP_TEXT, true, 7));
        assert_eq!(f.payload, b"Hello");
        let masked = [
            0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58,
        ];
        let (f, _) = parse_frame(&masked).unwrap().unwrap();
        assert_eq!(f.payload, b"Hello");
        assert_eq!(
            encode_frame(true, 0, OP_TEXT, b"Hello", Some([0x37, 0xfa, 0x21, 0x3d])),
            masked
        );
    }

    #[test]
    fn urls_parse() {
        let u = WsUrl::parse("ws://127.0.0.1:8083/mqtt").unwrap();
        assert_eq!(
            (u.host.as_str(), u.port, u.path.as_str()),
            ("127.0.0.1", 8083, "/mqtt")
        );
        let u = WsUrl::parse("ws://example.com").unwrap();
        assert_eq!((u.port, u.path.as_str()), (80, "/"));
        let u = WsUrl::parse("ws://[::1]:9001/a/b?x=1").unwrap();
        assert_eq!(
            (u.socket_addr().as_str(), u.path.as_str()),
            ("[::1]:9001", "/a/b?x=1")
        );
        assert!(WsUrl::parse("wss://h/mqtt").is_err());
        assert!(WsUrl::parse("http://h/mqtt").is_err());
        assert!(WsUrl::parse("ws://h:x/mqtt").is_err());
    }

    #[test]
    fn handshake_against_a_local_server() {
        use std::net::TcpListener;
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            let mut req = Vec::new();
            let mut b = [0u8; 1];
            while !req.ends_with(b"\r\n\r\n") {
                s.read_exact(&mut b).unwrap();
                req.push(b[0]);
            }
            let req = String::from_utf8(req).unwrap();
            assert!(req.contains("Sec-WebSocket-Protocol: mqtt\r\n"), "{req}");
            let key = req
                .lines()
                .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
                .unwrap()
                .to_owned();
            let resp = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
                 Sec-WebSocket-Accept: {}\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n",
                accept_key(&key)
            );
            let mut out = resp.into_bytes();
            // A fragmented binary message with a ping in the middle, then close.
            out.extend(encode_frame(false, 0, OP_BINARY, b"ab", None));
            out.extend(encode_frame(true, 0, OP_PING, b"p", None));
            out.extend(encode_frame(true, 0, OP_CONTINUATION, b"cd", None));
            out.extend(encode_frame(
                true,
                0,
                OP_CLOSE,
                &1000u16.to_be_bytes(),
                None,
            ));
            s.write_all(&out).unwrap();
            // Expect the pong and the close echo, both masked.
            let mut rx = Vec::new();
            let mut got = Vec::new();
            while got.len() < 2 {
                let mut tmp = [0u8; 256];
                let n = s.read(&mut tmp).unwrap();
                assert!(n > 0);
                rx.extend_from_slice(&tmp[..n]);
                while let Some((f, used)) = parse_frame(&rx).unwrap() {
                    rx.drain(..used);
                    got.push(f);
                }
            }
            assert!(got.iter().all(|f| f.masked));
            assert_eq!(
                (got[0].opcode, got[0].payload.as_slice()),
                (OP_PONG, &b"p"[..])
            );
            assert_eq!(got[1].opcode, OP_CLOSE);
        });
        let mut ws = WebSocket::connect(&format!("ws://127.0.0.1:{port}/mqtt"), &["mqtt"]).unwrap();
        assert_eq!(ws.subprotocol(), Some("mqtt"));
        assert_eq!(
            ws.next_message(Duration::from_secs(5)).unwrap(),
            WsEvent::Message {
                opcode: OP_BINARY,
                payload: b"abcd".to_vec()
            }
        );
        assert_eq!(
            ws.next_message(Duration::from_secs(5)).unwrap(),
            WsEvent::Closed
        );
        assert_eq!(ws.peer_close(), Some(CloseFrame { code: Some(1000) }));
        assert_eq!(ws.stats().pings, 1);
        server.join().unwrap();
    }
}
