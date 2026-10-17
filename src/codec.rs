// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! A small, independent MQTT 3.1 / 3.1.1 / 5.0 encoder and decoder.
//!
//! This module deliberately shares no code with the broker's codec, so a bug
//! there cannot hide itself here. The encoder never validates: it writes what
//! it is given (lengths saturate rather than fail), which lets tests build
//! deliberately malformed packets. The decoder is strict, because it checks
//! what the broker under test sends.

use std::fmt;

/// MQTT protocol version spoken by a test client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Version {
    /// MQTT 3.1 (`MQIsdp`, level 3).
    V31,
    /// MQTT 3.1.1 (`MQTT`, level 4).
    V311,
    /// MQTT 5.0 (`MQTT`, level 5).
    V5,
}

impl Version {
    /// Protocol name sent in CONNECT.
    #[must_use]
    pub const fn protocol_name(self) -> &'static str {
        match self {
            Self::V31 => "MQIsdp",
            Self::V311 | Self::V5 => "MQTT",
        }
    }

    /// Protocol level byte sent in CONNECT.
    #[must_use]
    pub const fn level(self) -> u8 {
        match self {
            Self::V31 => 3,
            Self::V311 => 4,
            Self::V5 => 5,
        }
    }

    /// Whether packets carry MQTT 5.0 properties and reason codes.
    #[must_use]
    pub const fn is_v5(self) -> bool {
        matches!(self, Self::V5)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::V31 => "3.1",
            Self::V311 => "3.1.1",
            Self::V5 => "5.0",
        })
    }
}

/// Control packet type numbers (upper nibble of the first byte).
pub mod packet_type {
    /// CONNECT
    pub const CONNECT: u8 = 1;
    /// CONNACK
    pub const CONNACK: u8 = 2;
    /// PUBLISH
    pub const PUBLISH: u8 = 3;
    /// PUBACK
    pub const PUBACK: u8 = 4;
    /// PUBREC
    pub const PUBREC: u8 = 5;
    /// PUBREL
    pub const PUBREL: u8 = 6;
    /// PUBCOMP
    pub const PUBCOMP: u8 = 7;
    /// SUBSCRIBE
    pub const SUBSCRIBE: u8 = 8;
    /// SUBACK
    pub const SUBACK: u8 = 9;
    /// UNSUBSCRIBE
    pub const UNSUBSCRIBE: u8 = 10;
    /// UNSUBACK
    pub const UNSUBACK: u8 = 11;
    /// PINGREQ
    pub const PINGREQ: u8 = 12;
    /// PINGRESP
    pub const PINGRESP: u8 = 13;
    /// DISCONNECT
    pub const DISCONNECT: u8 = 14;
    /// AUTH (5.0 only)
    pub const AUTH: u8 = 15;
}

/// MQTT 5.0 property identifiers.
pub mod prop {
    /// Payload Format Indicator (byte)
    pub const PAYLOAD_FORMAT_INDICATOR: u8 = 0x01;
    /// Message Expiry Interval (u32)
    pub const MESSAGE_EXPIRY_INTERVAL: u8 = 0x02;
    /// Content Type (string)
    pub const CONTENT_TYPE: u8 = 0x03;
    /// Response Topic (string)
    pub const RESPONSE_TOPIC: u8 = 0x08;
    /// Correlation Data (binary)
    pub const CORRELATION_DATA: u8 = 0x09;
    /// Subscription Identifier (varint)
    pub const SUBSCRIPTION_IDENTIFIER: u8 = 0x0B;
    /// Session Expiry Interval (u32)
    pub const SESSION_EXPIRY_INTERVAL: u8 = 0x11;
    /// Assigned Client Identifier (string)
    pub const ASSIGNED_CLIENT_IDENTIFIER: u8 = 0x12;
    /// Server Keep Alive (u16)
    pub const SERVER_KEEP_ALIVE: u8 = 0x13;
    /// Authentication Method (string)
    pub const AUTHENTICATION_METHOD: u8 = 0x15;
    /// Authentication Data (binary)
    pub const AUTHENTICATION_DATA: u8 = 0x16;
    /// Request Problem Information (byte)
    pub const REQUEST_PROBLEM_INFORMATION: u8 = 0x17;
    /// Will Delay Interval (u32)
    pub const WILL_DELAY_INTERVAL: u8 = 0x18;
    /// Request Response Information (byte)
    pub const REQUEST_RESPONSE_INFORMATION: u8 = 0x19;
    /// Response Information (string)
    pub const RESPONSE_INFORMATION: u8 = 0x1A;
    /// Server Reference (string)
    pub const SERVER_REFERENCE: u8 = 0x1C;
    /// Reason String (string)
    pub const REASON_STRING: u8 = 0x1F;
    /// Receive Maximum (u16)
    pub const RECEIVE_MAXIMUM: u8 = 0x21;
    /// Topic Alias Maximum (u16)
    pub const TOPIC_ALIAS_MAXIMUM: u8 = 0x22;
    /// Topic Alias (u16)
    pub const TOPIC_ALIAS: u8 = 0x23;
    /// Maximum QoS (byte)
    pub const MAXIMUM_QOS: u8 = 0x24;
    /// Retain Available (byte)
    pub const RETAIN_AVAILABLE: u8 = 0x25;
    /// User Property (string pair)
    pub const USER_PROPERTY: u8 = 0x26;
    /// Maximum Packet Size (u32)
    pub const MAXIMUM_PACKET_SIZE: u8 = 0x27;
    /// Wildcard Subscription Available (byte)
    pub const WILDCARD_SUBSCRIPTION_AVAILABLE: u8 = 0x28;
    /// Subscription Identifier Available (byte)
    pub const SUBSCRIPTION_IDENTIFIER_AVAILABLE: u8 = 0x29;
    /// Shared Subscription Available (byte)
    pub const SHARED_SUBSCRIPTION_AVAILABLE: u8 = 0x2A;
}

/// Largest value a Variable Byte Integer can carry.
pub const VARINT_MAX: u32 = 268_435_455;

/// Error raised while decoding bytes received from the broker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodecError(pub String);

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed packet from broker: {}", self.0)
    }
}

impl std::error::Error for CodecError {}

fn malformed<T>(msg: impl Into<String>) -> Result<T, CodecError> {
    Err(CodecError(msg.into()))
}

/// Value of an MQTT 5.0 property.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PropValue {
    /// Byte
    Byte(u8),
    /// Two Byte Integer
    TwoByte(u16),
    /// Four Byte Integer
    FourByte(u32),
    /// Variable Byte Integer
    VarInt(u32),
    /// UTF-8 Encoded String
    Str(String),
    /// Binary Data
    Bin(Vec<u8>),
    /// UTF-8 String Pair
    Pair(String, String),
}

/// One MQTT 5.0 property.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Property {
    /// Property identifier (see [`prop`]).
    pub id: u8,
    /// Property value.
    pub value: PropValue,
}

impl Property {
    /// Builds a property.
    #[must_use]
    pub const fn new(id: u8, value: PropValue) -> Self {
        Self { id, value }
    }
}

/// Returns the first property with identifier `id`.
#[must_use]
pub fn find_prop(props: &[Property], id: u8) -> Option<&PropValue> {
    props.iter().find(|p| p.id == id).map(|p| &p.value)
}

#[derive(Clone, Copy)]
enum Kind {
    Byte,
    Two,
    Four,
    Var,
    Str,
    Bin,
    Pair,
}

const fn prop_kind(id: u32) -> Option<Kind> {
    Some(match id {
        0x01 | 0x17 | 0x19 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2A => Kind::Byte,
        0x13 | 0x21 | 0x22 | 0x23 => Kind::Two,
        0x02 | 0x11 | 0x18 | 0x27 => Kind::Four,
        0x0B => Kind::Var,
        0x03 | 0x08 | 0x12 | 0x15 | 0x1A | 0x1C | 0x1F => Kind::Str,
        0x09 | 0x16 => Kind::Bin,
        0x26 => Kind::Pair,
        _ => return None,
    })
}

/// Byte writer used to build packet bodies, including malformed ones.
#[derive(Clone, Debug, Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    /// Creates an empty writer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one byte.
    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    /// Appends a big-endian two byte integer.
    pub fn u16(&mut self, v: u16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    /// Appends a big-endian four byte integer.
    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    /// Appends a Variable Byte Integer in minimal form. Values above
    /// [`VARINT_MAX`] produce a (malformed) five byte encoding.
    pub fn varint(&mut self, v: u32) -> &mut Self {
        let mut v = v;
        loop {
            let mut byte = u8::try_from(v % 128).unwrap_or(0);
            v /= 128;
            if v > 0 {
                byte |= 0x80;
            }
            self.buf.push(byte);
            if v == 0 {
                return self;
            }
        }
    }

    /// Appends a length-prefixed UTF-8 string. Lengths above 65535 saturate
    /// (producing a malformed field).
    pub fn str(&mut self, s: &str) -> &mut Self {
        self.bin(s.as_bytes())
    }

    /// Appends length-prefixed binary data (saturating length).
    pub fn bin(&mut self, b: &[u8]) -> &mut Self {
        self.u16(u16::try_from(b.len()).unwrap_or(u16::MAX));
        self.buf.extend_from_slice(b);
        self
    }

    /// Appends raw bytes.
    pub fn raw(&mut self, b: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(b);
        self
    }

    /// Appends an MQTT 5.0 property block (length + properties).
    pub fn properties(&mut self, props: &[Property]) -> &mut Self {
        let mut inner = Writer::new();
        for p in props {
            inner.varint(u32::from(p.id));
            match &p.value {
                PropValue::Byte(v) => inner.u8(*v),
                PropValue::TwoByte(v) => inner.u16(*v),
                PropValue::FourByte(v) => inner.u32(*v),
                PropValue::VarInt(v) => inner.varint(*v),
                PropValue::Str(s) => inner.str(s),
                PropValue::Bin(b) => inner.bin(b),
                PropValue::Pair(k, v) => inner.str(k).str(v),
            };
        }
        self.varint(u32::try_from(inner.buf.len()).unwrap_or(u32::MAX));
        self.buf.extend_from_slice(&inner.buf);
        self
    }

    /// Bytes written so far.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Wraps the body in a fixed header with the given first byte and a
    /// correct Remaining Length.
    #[must_use]
    pub fn frame(&self, first_byte: u8) -> Vec<u8> {
        frame(first_byte, &self.buf)
    }
}

/// Builds a packet from a first byte and a body, computing Remaining Length.
#[must_use]
pub fn frame(first_byte: u8, body: &[u8]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u8(first_byte)
        .varint(u32::try_from(body.len()).unwrap_or(u32::MAX))
        .raw(body);
    w.buf
}

/// Will message carried in CONNECT.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Will {
    /// Will Topic.
    pub topic: String,
    /// Will Payload.
    pub payload: Vec<u8>,
    /// Will QoS (written as-is into the flags, so 3 is possible).
    pub qos: u8,
    /// Will Retain.
    pub retain: bool,
    /// Will Properties (5.0 only).
    pub properties: Vec<Property>,
}

impl Will {
    /// A QoS 0, non-retained will.
    #[must_use]
    pub fn new(topic: &str, payload: &[u8]) -> Self {
        Self {
            topic: topic.to_owned(),
            payload: payload.to_vec(),
            qos: 0,
            retain: false,
            properties: Vec::new(),
        }
    }
}

/// CONNECT packet. Built with [`Connect::new`] and the builder methods.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connect {
    /// Version the client decodes the broker's replies with.
    pub version: Version,
    /// Protocol Name (defaults from `version`).
    pub protocol_name: String,
    /// Protocol Level (defaults from `version`).
    pub protocol_level: u8,
    /// Clean Session (3.x) / Clean Start (5.0).
    pub clean_start: bool,
    /// Keep Alive in seconds.
    pub keep_alive: u16,
    /// CONNECT properties (written only when `protocol_level` is 5).
    pub properties: Vec<Property>,
    /// Client Identifier.
    pub client_id: String,
    /// Will message.
    pub will: Option<Will>,
    /// User Name.
    pub username: Option<String>,
    /// Password.
    pub password: Option<Vec<u8>>,
    /// Replaces the computed Connect Flags byte (for malformed packets).
    pub flags_override: Option<u8>,
}

impl Connect {
    /// A clean-session CONNECT with a 60 s keep-alive.
    #[must_use]
    pub fn new(version: Version, client_id: &str) -> Self {
        Self {
            version,
            protocol_name: version.protocol_name().to_owned(),
            protocol_level: version.level(),
            clean_start: true,
            keep_alive: 60,
            properties: Vec::new(),
            client_id: client_id.to_owned(),
            will: None,
            username: None,
            password: None,
            flags_override: None,
        }
    }

    /// Sets Clean Session / Clean Start.
    #[must_use]
    pub fn clean(mut self, clean: bool) -> Self {
        self.clean_start = clean;
        self
    }

    /// Sets Keep Alive.
    #[must_use]
    pub fn keep_alive(mut self, secs: u16) -> Self {
        self.keep_alive = secs;
        self
    }

    /// Sets the will message.
    #[must_use]
    pub fn will(mut self, will: Will) -> Self {
        self.will = Some(will);
        self
    }

    /// Sets the user name.
    #[must_use]
    pub fn username(mut self, name: &str) -> Self {
        self.username = Some(name.to_owned());
        self
    }

    /// Sets the password.
    #[must_use]
    pub fn password(mut self, pw: &[u8]) -> Self {
        self.password = Some(pw.to_vec());
        self
    }

    /// Adds a CONNECT property (5.0).
    #[must_use]
    pub fn property(mut self, id: u8, value: PropValue) -> Self {
        self.properties.push(Property::new(id, value));
        self
    }

    /// Overrides protocol name and level.
    #[must_use]
    pub fn protocol(mut self, name: &str, level: u8) -> Self {
        name.clone_into(&mut self.protocol_name);
        self.protocol_level = level;
        self
    }

    /// Overrides the Connect Flags byte.
    #[must_use]
    pub fn flags(mut self, flags: u8) -> Self {
        self.flags_override = Some(flags);
        self
    }

    /// Connect Flags byte derived from the fields (ignores the override).
    #[must_use]
    pub fn computed_flags(&self) -> u8 {
        let mut f = 0u8;
        if self.clean_start {
            f |= 0x02;
        }
        if let Some(w) = &self.will {
            f |= 0x04 | ((w.qos & 0x03) << 3);
            if w.retain {
                f |= 0x20;
            }
        }
        if self.password.is_some() {
            f |= 0x40;
        }
        if self.username.is_some() {
            f |= 0x80;
        }
        f
    }

    fn encode_body(&self) -> Writer {
        let v5 = self.protocol_level == 5;
        let mut w = Writer::new();
        w.str(&self.protocol_name)
            .u8(self.protocol_level)
            .u8(self.flags_override.unwrap_or_else(|| self.computed_flags()))
            .u16(self.keep_alive);
        if v5 {
            w.properties(&self.properties);
        }
        w.str(&self.client_id);
        if let Some(will) = &self.will {
            if v5 {
                w.properties(&will.properties);
            }
            w.str(&will.topic).bin(&will.payload);
        }
        if let Some(u) = &self.username {
            w.str(u);
        }
        if let Some(p) = &self.password {
            w.bin(p);
        }
        w
    }
}

/// CONNACK packet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnAck {
    /// Raw Connect Acknowledge Flags byte (bit 0 = Session Present).
    pub flags: u8,
    /// Return code (3.x) or Reason Code (5.0).
    pub code: u8,
    /// CONNACK properties (5.0).
    pub properties: Vec<Property>,
}

impl ConnAck {
    /// Session Present flag.
    #[must_use]
    pub const fn session_present(&self) -> bool {
        self.flags & 0x01 != 0
    }
}

/// PUBLISH packet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publish {
    /// DUP flag.
    pub dup: bool,
    /// QoS (written as-is, so 3 is possible).
    pub qos: u8,
    /// RETAIN flag.
    pub retain: bool,
    /// Topic Name.
    pub topic: String,
    /// Packet Identifier (present when QoS > 0).
    pub packet_id: Option<u16>,
    /// PUBLISH properties (5.0).
    pub properties: Vec<Property>,
    /// Application payload.
    pub payload: Vec<u8>,
}

impl Publish {
    /// A QoS 0 PUBLISH.
    #[must_use]
    pub fn new(topic: &str, payload: &[u8]) -> Self {
        Self {
            dup: false,
            qos: 0,
            retain: false,
            topic: topic.to_owned(),
            packet_id: None,
            properties: Vec::new(),
            payload: payload.to_vec(),
        }
    }
}

/// PUBACK / PUBREC / PUBREL / PUBCOMP.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ack {
    /// Packet Identifier.
    pub packet_id: u16,
    /// Reason Code (5.0; absent means 0x00).
    pub reason: Option<u8>,
    /// Properties (5.0).
    pub properties: Vec<Property>,
}

impl Ack {
    /// An acknowledgement with no reason code.
    #[must_use]
    pub const fn new(packet_id: u16) -> Self {
        Self {
            packet_id,
            reason: None,
            properties: Vec::new(),
        }
    }
}

/// SUBSCRIBE packet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscribe {
    /// Packet Identifier.
    pub packet_id: u16,
    /// Properties (5.0).
    pub properties: Vec<Property>,
    /// Topic filters with their requested QoS / subscription options byte.
    pub filters: Vec<(String, u8)>,
}

/// SUBACK or UNSUBACK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubAck {
    /// Packet Identifier.
    pub packet_id: u16,
    /// Properties (5.0).
    pub properties: Vec<Property>,
    /// Return / reason codes (empty for a 3.x UNSUBACK).
    pub codes: Vec<u8>,
}

/// UNSUBSCRIBE packet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unsubscribe {
    /// Packet Identifier.
    pub packet_id: u16,
    /// Properties (5.0).
    pub properties: Vec<Property>,
    /// Topic filters.
    pub filters: Vec<String>,
}

/// DISCONNECT or AUTH (reason code + properties; both empty in 3.x).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ReasonOnly {
    /// Reason Code (absent means 0x00).
    pub reason: Option<u8>,
    /// Properties (5.0).
    pub properties: Vec<Property>,
}

/// Any MQTT control packet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet {
    /// CONNECT
    Connect(Box<Connect>),
    /// CONNACK
    ConnAck(ConnAck),
    /// PUBLISH
    Publish(Publish),
    /// PUBACK
    PubAck(Ack),
    /// PUBREC
    PubRec(Ack),
    /// PUBREL
    PubRel(Ack),
    /// PUBCOMP
    PubComp(Ack),
    /// SUBSCRIBE
    Subscribe(Subscribe),
    /// SUBACK
    SubAck(SubAck),
    /// UNSUBSCRIBE
    Unsubscribe(Unsubscribe),
    /// UNSUBACK
    UnsubAck(SubAck),
    /// PINGREQ
    PingReq,
    /// PINGRESP
    PingResp,
    /// DISCONNECT
    Disconnect(ReasonOnly),
    /// AUTH (5.0)
    Auth(ReasonOnly),
}

impl Packet {
    /// Short packet name for diagnostics.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Connect(_) => "CONNECT",
            Self::ConnAck(_) => "CONNACK",
            Self::Publish(_) => "PUBLISH",
            Self::PubAck(_) => "PUBACK",
            Self::PubRec(_) => "PUBREC",
            Self::PubRel(_) => "PUBREL",
            Self::PubComp(_) => "PUBCOMP",
            Self::Subscribe(_) => "SUBSCRIBE",
            Self::SubAck(_) => "SUBACK",
            Self::Unsubscribe(_) => "UNSUBSCRIBE",
            Self::UnsubAck(_) => "UNSUBACK",
            Self::PingReq => "PINGREQ",
            Self::PingResp => "PINGRESP",
            Self::Disconnect(_) => "DISCONNECT",
            Self::Auth(_) => "AUTH",
        }
    }

    /// Encodes the packet. `version` selects 5.0 properties / reason codes for
    /// every packet except CONNECT, whose own `protocol_level` decides.
    #[must_use]
    pub fn encode(&self, version: Version) -> Vec<u8> {
        use packet_type as t;
        let v5 = version.is_v5();
        let mut w = Writer::new();
        let first = match self {
            Self::Connect(c) => {
                w = c.encode_body();
                t::CONNECT << 4
            }
            Self::ConnAck(c) => {
                w.u8(c.flags).u8(c.code);
                if v5 {
                    w.properties(&c.properties);
                }
                t::CONNACK << 4
            }
            Self::Publish(p) => {
                w.str(&p.topic);
                if let Some(id) = p.packet_id {
                    w.u16(id);
                }
                if v5 {
                    w.properties(&p.properties);
                }
                w.raw(&p.payload);
                (t::PUBLISH << 4)
                    | (u8::from(p.dup) << 3)
                    | ((p.qos & 0x03) << 1)
                    | u8::from(p.retain)
            }
            Self::PubAck(a) => encode_ack(&mut w, a, v5, t::PUBACK << 4),
            Self::PubRec(a) => encode_ack(&mut w, a, v5, t::PUBREC << 4),
            Self::PubRel(a) => encode_ack(&mut w, a, v5, (t::PUBREL << 4) | 0x02),
            Self::PubComp(a) => encode_ack(&mut w, a, v5, t::PUBCOMP << 4),
            Self::Subscribe(s) => {
                w.u16(s.packet_id);
                if v5 {
                    w.properties(&s.properties);
                }
                for (f, o) in &s.filters {
                    w.str(f).u8(*o);
                }
                (t::SUBSCRIBE << 4) | 0x02
            }
            Self::SubAck(s) | Self::UnsubAck(s) => {
                w.u16(s.packet_id);
                if v5 {
                    w.properties(&s.properties);
                }
                w.raw(&s.codes);
                if matches!(self, Self::SubAck(_)) {
                    t::SUBACK << 4
                } else {
                    t::UNSUBACK << 4
                }
            }
            Self::Unsubscribe(u) => {
                w.u16(u.packet_id);
                if v5 {
                    w.properties(&u.properties);
                }
                for f in &u.filters {
                    w.str(f);
                }
                (t::UNSUBSCRIBE << 4) | 0x02
            }
            Self::PingReq => t::PINGREQ << 4,
            Self::PingResp => t::PINGRESP << 4,
            Self::Disconnect(d) | Self::Auth(d) => {
                if v5 && (d.reason.is_some() || !d.properties.is_empty()) {
                    w.u8(d.reason.unwrap_or(0));
                    if !d.properties.is_empty() {
                        w.properties(&d.properties);
                    }
                }
                if matches!(self, Self::Auth(_)) {
                    t::AUTH << 4
                } else {
                    t::DISCONNECT << 4
                }
            }
        };
        w.frame(first)
    }
}

fn encode_ack(w: &mut Writer, a: &Ack, v5: bool, first: u8) -> u8 {
    w.u16(a.packet_id);
    if v5 && (a.reason.is_some() || !a.properties.is_empty()) {
        w.u8(a.reason.unwrap_or(0));
        if !a.properties.is_empty() {
            w.properties(&a.properties);
        }
    }
    first
}

/// Cursor over a packet body.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let end = self.pos.checked_add(n).filter(|e| *e <= self.buf.len());
        match end.and_then(|e| self.buf.get(self.pos..e).map(|s| (e, s))) {
            Some((e, s)) => {
                self.pos = e;
                Ok(s)
            }
            None => malformed(format!("field of {n} bytes overruns packet")),
        }
    }

    fn rest(&mut self) -> &'a [u8] {
        let s = self.buf.get(self.pos..).unwrap_or(&[]);
        self.pos = self.buf.len();
        s
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?.first().copied().unwrap_or(0))
    }

    fn u16(&mut self) -> Result<u16, CodecError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([
            b.first().copied().unwrap_or(0),
            b.get(1).copied().unwrap_or(0),
        ]))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        let b = self.take(4)?;
        let mut a = [0u8; 4];
        a.copy_from_slice(b);
        Ok(u32::from_be_bytes(a))
    }

    fn varint(&mut self) -> Result<u32, CodecError> {
        match decode_varint(self.buf.get(self.pos..).unwrap_or(&[]))? {
            Some((v, n)) => {
                self.pos += n;
                Ok(v)
            }
            None => malformed("truncated variable byte integer"),
        }
    }

    fn bin(&mut self) -> Result<Vec<u8>, CodecError> {
        let n = self.u16()?;
        Ok(self.take(usize::from(n))?.to_vec())
    }

    fn str(&mut self) -> Result<String, CodecError> {
        let b = self.bin()?;
        let s = String::from_utf8(b).or_else(|_| malformed("string is not valid UTF-8"))?;
        if s.contains('\0') {
            return malformed("string contains U+0000");
        }
        Ok(s)
    }

    fn properties(&mut self) -> Result<Vec<Property>, CodecError> {
        let len = usize::try_from(self.varint()?).unwrap_or(usize::MAX);
        let mut r = Reader::new(self.take(len)?);
        let mut out = Vec::new();
        while r.remaining() > 0 {
            let id = r.varint()?;
            let value = match prop_kind(id) {
                Some(Kind::Byte) => PropValue::Byte(r.u8()?),
                Some(Kind::Two) => PropValue::TwoByte(r.u16()?),
                Some(Kind::Four) => PropValue::FourByte(r.u32()?),
                Some(Kind::Var) => PropValue::VarInt(r.varint()?),
                Some(Kind::Str) => PropValue::Str(r.str()?),
                Some(Kind::Bin) => PropValue::Bin(r.bin()?),
                Some(Kind::Pair) => PropValue::Pair(r.str()?, r.str()?),
                None => return malformed(format!("unknown property identifier {id:#04x}")),
            };
            let id = u8::try_from(id).or_else(|_| malformed("property id out of range"))?;
            out.push(Property { id, value });
        }
        Ok(out)
    }

    fn finish(&self, what: &str) -> Result<(), CodecError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            malformed(format!("{} trailing bytes in {what}", self.remaining()))
        }
    }
}

/// Decodes a Variable Byte Integer. `Ok(None)` means more bytes are needed;
/// an error means a fifth length byte (or a non-minimal encoding) was seen.
pub fn decode_varint(buf: &[u8]) -> Result<Option<(u32, usize)>, CodecError> {
    let mut value = 0u32;
    for (i, b) in buf.iter().enumerate().take(4) {
        value |= u32::from(b & 0x7F) << (7 * i);
        if b & 0x80 == 0 {
            if i > 0 && *b == 0 {
                return malformed("non-minimal variable byte integer");
            }
            return Ok(Some((value, i + 1)));
        }
    }
    if buf.len() >= 4 {
        malformed("variable byte integer longer than 4 bytes")
    } else {
        Ok(None)
    }
}

/// Decodes a Remaining Length. Only 5.0 requires the minimal encoding
/// (MQTT-1.5.5-1); 3.1 and 3.1.1 limit the field to four bytes, so a
/// non-minimal one is accepted there.
fn decode_remaining_length(
    buf: &[u8],
    version: Version,
) -> Result<Option<(u32, usize)>, CodecError> {
    if version.is_v5() {
        return decode_varint(buf);
    }
    let mut value = 0u32;
    for (i, b) in buf.iter().enumerate().take(4) {
        value |= u32::from(b & 0x7F) << (7 * i);
        if b & 0x80 == 0 {
            return Ok(Some((value, i + 1)));
        }
    }
    if buf.len() >= 4 {
        malformed("Remaining Length longer than 4 bytes")
    } else {
        Ok(None)
    }
}

/// Decodes one packet from the front of `buf`. Returns the packet and the
/// number of bytes it occupied, or `Ok(None)` if `buf` holds only part of a
/// packet.
pub fn decode(buf: &[u8], version: Version) -> Result<Option<(Packet, usize)>, CodecError> {
    let Some(&first) = buf.first() else {
        return Ok(None);
    };
    let Some((len, n)) = decode_remaining_length(buf.get(1..).unwrap_or(&[]), version)? else {
        return Ok(None);
    };
    let total = 1 + n + usize::try_from(len).unwrap_or(usize::MAX);
    let Some(body) = buf.get(1 + n..total) else {
        return Ok(None);
    };
    let packet = decode_body(first, body, version)
        .and_then(|p| {
            if version.is_v5() {
                check_v5_properties(&p)?;
            }
            Ok(p)
        })
        .map_err(|e| {
            let bytes = buf.get(..total.min(32)).unwrap_or(&[]);
            let more = if total > 32 { " .." } else { "" };
            CodecError(format!("{} (bytes {bytes:02x?}{more})", e.0))
        })?;
    Ok(Some((packet, total)))
}

/// Packets whose properties [`check_v5_properties`] validates: those a 5.0
/// server sends (CONNECT, SUBSCRIBE and UNSUBSCRIBE are client packets).
#[derive(Clone, Copy, PartialEq, Eq)]
enum PropCtx {
    ConnAck,
    Publish,
    Ack,
    SubAck,
    Disconnect,
    Auth,
}

/// Whether property `id` may appear in a packet of kind `ctx` (5.0 Table 2-4).
const fn property_allowed(id: u8, ctx: PropCtx) -> bool {
    use PropCtx::{Auth, ConnAck, Disconnect, Publish};
    match id {
        0x26 => true,
        0x01 | 0x02 | 0x03 | 0x08 | 0x09 | 0x0B | 0x23 => matches!(ctx, Publish),
        0x11 | 0x1C => matches!(ctx, ConnAck | Disconnect),
        0x15 | 0x16 => matches!(ctx, ConnAck | Auth),
        0x12 | 0x13 | 0x1A | 0x21 | 0x22 | 0x24 | 0x25 | 0x27 | 0x28 | 0x29 | 0x2A => {
            matches!(ctx, ConnAck)
        }
        0x1F => !matches!(ctx, Publish),
        _ => false,
    }
}

/// Checks the properties of a packet a 5.0 server sent: each identifier valid
/// for the packet type and present at most once (User Property, and
/// Subscription Identifier in PUBLISH, may repeat), Subscription Identifier
/// non-zero, and the 0/1 and non-zero value ranges the spec fixes
/// (§2.2.2.2, §3.2.2.3, §3.3.2.3). A violation is a Malformed Packet or a
/// Protocol Error on the server's part.
fn check_v5_properties(packet: &Packet) -> Result<(), CodecError> {
    let (ctx, props) = match packet {
        Packet::ConnAck(c) => (PropCtx::ConnAck, &c.properties),
        Packet::Publish(p) => (PropCtx::Publish, &p.properties),
        Packet::PubAck(a) | Packet::PubRec(a) | Packet::PubRel(a) | Packet::PubComp(a) => {
            (PropCtx::Ack, &a.properties)
        }
        Packet::SubAck(a) | Packet::UnsubAck(a) => (PropCtx::SubAck, &a.properties),
        Packet::Disconnect(d) => (PropCtx::Disconnect, &d.properties),
        Packet::Auth(a) => (PropCtx::Auth, &a.properties),
        _ => return Ok(()),
    };
    let name = packet.name();
    let mut seen = [false; 0x2B];
    for p in props {
        if !property_allowed(p.id, ctx) {
            return malformed(format!("property {:#04x} is not valid in {name}", p.id));
        }
        let repeatable = p.id == 0x26 || (p.id == 0x0B && ctx == PropCtx::Publish);
        let slot = seen.get_mut(usize::from(p.id));
        match slot {
            Some(s) if *s && !repeatable => {
                return malformed(format!("property {:#04x} more than once in {name}", p.id));
            }
            Some(s) => *s = true,
            None => {}
        }
        match (p.id, &p.value) {
            (0x0B, PropValue::VarInt(0)) => {
                return malformed(format!("Subscription Identifier 0 in {name}"));
            }
            (0x01 | 0x25 | 0x28 | 0x29 | 0x2A, PropValue::Byte(v)) if *v > 1 => {
                return malformed(format!("property {:#04x} = {v} in {name} (0 or 1)", p.id));
            }
            (0x21, PropValue::TwoByte(0)) | (0x27, PropValue::FourByte(0)) => {
                return malformed(format!("property {:#04x} = 0 in {name}", p.id));
            }
            _ => {}
        }
    }
    Ok(())
}

fn expect_flags(first: u8, allowed: &[u8], name: &str) -> Result<(), CodecError> {
    if allowed.contains(&(first & 0x0F)) {
        Ok(())
    } else {
        malformed(format!(
            "{name} with invalid fixed-header flags {first:#04x}"
        ))
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one flat match over all packet types"
)]
fn decode_body(first: u8, body: &[u8], version: Version) -> Result<Packet, CodecError> {
    use packet_type as t;
    let v5 = version.is_v5();
    let mut r = Reader::new(body);
    let kind = first >> 4;
    // 3.1 permits DUP on PUBREL, SUBSCRIBE and UNSUBSCRIBE.
    let qos1: &[u8] = if version == Version::V31 {
        &[0x02, 0x0A]
    } else {
        &[0x02]
    };
    let packet = match kind {
        t::CONNECT => {
            expect_flags(first, &[0], "CONNECT")?;
            Packet::Connect(Box::new(decode_connect(&mut r)?))
        }
        t::CONNACK => {
            expect_flags(first, &[0], "CONNACK")?;
            let flags = r.u8()?;
            // Connect Acknowledge Flags bits 7-1 are reserved, set to 0
            // (3.1.1 §3.2.2.1, 5.0 §3.2.2.1); 3.1 leaves the byte unused.
            if version != Version::V31 && flags & 0xFE != 0 {
                return malformed(format!("CONNACK flags {flags:#04x}: reserved bits set"));
            }
            let code = r.u8()?;
            // A 5.0 server may answer an unsupported level with a 2-byte CONNACK.
            let properties = if v5 && r.remaining() > 0 {
                r.properties()?
            } else {
                Vec::new()
            };
            Packet::ConnAck(ConnAck {
                flags,
                code,
                properties,
            })
        }
        t::PUBLISH => {
            let qos = (first >> 1) & 0x03;
            if qos == 3 {
                return malformed("PUBLISH with QoS 3");
            }
            let topic = r.str()?;
            let packet_id = if qos > 0 { Some(r.u16()?) } else { None };
            // 3.1.1 MQTT-2.3.1-1 / 5.0 MQTT-2.2.1-3: a non-zero identifier;
            // MQTT-3.3.1-2: DUP 0 at QoS 0 (3.1 only describes these).
            if version != Version::V31 {
                if packet_id == Some(0) {
                    return malformed("PUBLISH with QoS > 0 and Packet Identifier 0");
                }
                if qos == 0 && first & 0x08 != 0 {
                    return malformed("PUBLISH with QoS 0 and DUP 1");
                }
            }
            let properties = if v5 { r.properties()? } else { Vec::new() };
            Packet::Publish(Publish {
                dup: first & 0x08 != 0,
                qos,
                retain: first & 0x01 != 0,
                topic,
                packet_id,
                properties,
                payload: r.rest().to_vec(),
            })
        }
        t::PUBACK | t::PUBREC | t::PUBREL | t::PUBCOMP => {
            if kind == t::PUBREL {
                expect_flags(first, qos1, "PUBREL")?;
            } else {
                expect_flags(first, &[0], "acknowledgement")?;
            }
            let packet_id = r.u16()?;
            let (reason, properties) = decode_reason(&mut r, v5)?;
            let a = Ack {
                packet_id,
                reason,
                properties,
            };
            match kind {
                t::PUBACK => Packet::PubAck(a),
                t::PUBREC => Packet::PubRec(a),
                t::PUBREL => Packet::PubRel(a),
                _ => Packet::PubComp(a),
            }
        }
        t::SUBSCRIBE => {
            expect_flags(first, qos1, "SUBSCRIBE")?;
            let packet_id = r.u16()?;
            let properties = if v5 { r.properties()? } else { Vec::new() };
            let mut filters = Vec::new();
            while r.remaining() > 0 {
                filters.push((r.str()?, r.u8()?));
            }
            Packet::Subscribe(Subscribe {
                packet_id,
                properties,
                filters,
            })
        }
        t::SUBACK | t::UNSUBACK => {
            expect_flags(first, &[0], "SUBACK/UNSUBACK")?;
            let packet_id = r.u16()?;
            let properties = if v5 { r.properties()? } else { Vec::new() };
            // A 3.x UNSUBACK has no payload (Remaining Length 2).
            if kind == t::UNSUBACK && !v5 && r.remaining() > 0 {
                return malformed(format!(
                    "{} bytes of payload in a 3.x UNSUBACK",
                    r.remaining()
                ));
            }
            let s = SubAck {
                packet_id,
                properties,
                codes: r.rest().to_vec(),
            };
            if kind == t::SUBACK {
                Packet::SubAck(s)
            } else {
                Packet::UnsubAck(s)
            }
        }
        t::UNSUBSCRIBE => {
            expect_flags(first, qos1, "UNSUBSCRIBE")?;
            let packet_id = r.u16()?;
            let properties = if v5 { r.properties()? } else { Vec::new() };
            let mut filters = Vec::new();
            while r.remaining() > 0 {
                filters.push(r.str()?);
            }
            Packet::Unsubscribe(Unsubscribe {
                packet_id,
                properties,
                filters,
            })
        }
        t::PINGREQ | t::PINGRESP => {
            expect_flags(first, &[0], "PINGREQ/PINGRESP")?;
            r.finish("PINGREQ/PINGRESP")?;
            if kind == t::PINGREQ {
                Packet::PingReq
            } else {
                Packet::PingResp
            }
        }
        t::DISCONNECT => {
            expect_flags(first, &[0], "DISCONNECT")?;
            // 3.x defines no server-to-client DISCONNECT. Some servers send a
            // 5.0-style one (reason code, properties) to 3.x clients before
            // closing; decode it so cases can tell it from other packets.
            let (reason, properties) = decode_reason(&mut r, true)?;
            Packet::Disconnect(ReasonOnly { reason, properties })
        }
        t::AUTH if v5 => {
            expect_flags(first, &[0], "AUTH")?;
            let (reason, properties) = decode_reason(&mut r, v5)?;
            Packet::Auth(ReasonOnly { reason, properties })
        }
        _ => return malformed(format!("reserved packet type {kind}")),
    };
    r.finish(packet.name())?;
    Ok(packet)
}

fn decode_reason(r: &mut Reader<'_>, v5: bool) -> Result<(Option<u8>, Vec<Property>), CodecError> {
    if !v5 || r.remaining() == 0 {
        return Ok((None, Vec::new()));
    }
    let reason = r.u8()?;
    let props = if r.remaining() > 0 {
        r.properties()?
    } else {
        Vec::new()
    };
    Ok((Some(reason), props))
}

fn decode_connect(r: &mut Reader<'_>) -> Result<Connect, CodecError> {
    let protocol_name = r.str()?;
    let protocol_level = r.u8()?;
    let version = match protocol_level {
        3 => Version::V31,
        5 => Version::V5,
        _ => Version::V311,
    };
    let flags = r.u8()?;
    let keep_alive = r.u16()?;
    let v5 = protocol_level == 5;
    let properties = if v5 { r.properties()? } else { Vec::new() };
    let client_id = r.str()?;
    let will = if flags & 0x04 != 0 {
        let properties = if v5 { r.properties()? } else { Vec::new() };
        Some(Will {
            topic: r.str()?,
            payload: r.bin()?,
            qos: (flags >> 3) & 0x03,
            retain: flags & 0x20 != 0,
            properties,
        })
    } else {
        None
    };
    let username = if flags & 0x80 != 0 {
        Some(r.str()?)
    } else {
        None
    };
    let password = if flags & 0x40 != 0 {
        Some(r.bin()?)
    } else {
        None
    };
    let mut c = Connect {
        version,
        protocol_name,
        protocol_level,
        clean_start: flags & 0x02 != 0,
        keep_alive,
        properties,
        client_id,
        will,
        username,
        password,
        flags_override: None,
    };
    if c.computed_flags() != flags {
        c.flags_override = Some(flags);
    }
    Ok(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(p: &Packet, v: Version) {
        let bytes = p.encode(v);
        let (d, n) = decode(&bytes, v).unwrap().unwrap();
        assert_eq!(n, bytes.len());
        assert_eq!(&d, p, "{v}");
    }

    #[test]
    fn varint_boundaries() {
        for (v, len) in [
            (0u32, 1usize),
            (127, 1),
            (128, 2),
            (16_383, 2),
            (16_384, 3),
            (2_097_151, 3),
            (2_097_152, 4),
            (VARINT_MAX, 4),
        ] {
            let mut w = Writer::new();
            w.varint(v);
            assert_eq!(w.as_bytes().len(), len);
            assert_eq!(decode_varint(w.as_bytes()).unwrap(), Some((v, len)));
        }
        let mut w = Writer::new();
        w.varint(VARINT_MAX + 1);
        assert_eq!(w.as_bytes().len(), 5);
        assert!(decode_varint(w.as_bytes()).is_err());
        assert!(decode_varint(&[0x80, 0x00]).is_err());
        assert_eq!(decode_varint(&[0x80]).unwrap(), None);
    }

    #[test]
    fn partial_packets_need_more() {
        let bytes = Packet::Publish(Publish::new("a/b", b"hello")).encode(Version::V311);
        for cut in 0..bytes.len() {
            assert_eq!(decode(&bytes[..cut], Version::V311).unwrap(), None);
        }
    }

    #[test]
    fn v5_properties_are_checked_against_the_packet_type() {
        let decode5 = |p: &Packet| decode(&p.encode(Version::V5), Version::V5);
        let with = |id, value| ConnAck {
            flags: 0,
            code: 0,
            properties: vec![Property::new(id, value)],
        };
        // Topic Alias belongs to PUBLISH only.
        assert!(
            decode5(&Packet::ConnAck(with(
                prop::TOPIC_ALIAS,
                PropValue::TwoByte(1)
            )))
            .is_err()
        );
        // Retain Available is 0 or 1.
        assert!(
            decode5(&Packet::ConnAck(with(
                prop::RETAIN_AVAILABLE,
                PropValue::Byte(2)
            )))
            .is_err()
        );
        assert!(
            decode5(&Packet::ConnAck(with(
                prop::RETAIN_AVAILABLE,
                PropValue::Byte(1)
            )))
            .is_ok()
        );
        // A single-instance property twice.
        let mut twice = with(prop::SERVER_KEEP_ALIVE, PropValue::TwoByte(5));
        twice.properties.push(twice.properties[0].clone());
        assert!(decode5(&Packet::ConnAck(twice)).is_err());
        // Subscription Identifier 0 in PUBLISH.
        let mut p = Publish::new("t", b"x");
        p.properties = vec![Property::new(
            prop::SUBSCRIPTION_IDENTIFIER,
            PropValue::VarInt(0),
        )];
        assert!(decode5(&Packet::Publish(p)).is_err());
        // Packet Identifier 0 at QoS 1, and DUP at QoS 0 (3.1.1 and 5.0).
        for v in [Version::V311, Version::V5] {
            let mut p = Publish::new("t", b"x");
            p.qos = 1;
            p.packet_id = Some(0);
            assert!(decode(&Packet::Publish(p).encode(v), v).is_err());
            let mut p = Publish::new("t", b"x");
            p.dup = true;
            assert!(decode(&Packet::Publish(p).encode(v), v).is_err());
        }
    }

    #[test]
    fn roundtrip_all_packets() {
        // Valid in every server packet but PUBLISH (5.0 Table 2-4).
        let props = vec![
            Property::new(prop::REASON_STRING, PropValue::Str("why".into())),
            Property::new(prop::USER_PROPERTY, PropValue::Pair("k".into(), "v".into())),
        ];
        let publish_props = vec![
            Property::new(prop::USER_PROPERTY, PropValue::Pair("k".into(), "v".into())),
            Property::new(prop::SUBSCRIPTION_IDENTIFIER, PropValue::VarInt(300)),
            Property::new(prop::SUBSCRIPTION_IDENTIFIER, PropValue::VarInt(7)),
        ];
        for v in [Version::V31, Version::V311, Version::V5] {
            let p = |x: &Vec<Property>| if v.is_v5() { x.clone() } else { Vec::new() };
            let mut will = Will::new("w/t", b"bye");
            will.qos = 2;
            will.retain = true;
            will.properties = p(&props);
            let mut c = Connect::new(v, "cid")
                .clean(false)
                .keep_alive(7)
                .will(will)
                .username("u")
                .password(b"p");
            c.properties = p(&props);
            roundtrip(&Packet::Connect(Box::new(c)), v);
            roundtrip(
                &Packet::ConnAck(ConnAck {
                    flags: 1,
                    code: 0,
                    properties: p(&props),
                }),
                v,
            );
            let mut publ = Publish::new("t/1", b"payload");
            publ.qos = 1;
            publ.dup = true;
            publ.retain = true;
            publ.packet_id = Some(9);
            publ.properties = p(&publish_props);
            roundtrip(&Packet::Publish(publ), v);
            let ack = if v.is_v5() {
                Ack {
                    packet_id: 5,
                    reason: Some(0x10),
                    properties: props.clone(),
                }
            } else {
                Ack::new(5)
            };
            roundtrip(&Packet::PubAck(ack.clone()), v);
            roundtrip(&Packet::PubRec(ack.clone()), v);
            roundtrip(&Packet::PubRel(ack.clone()), v);
            roundtrip(&Packet::PubComp(ack), v);
            roundtrip(
                &Packet::Subscribe(Subscribe {
                    packet_id: 3,
                    properties: p(&props),
                    filters: vec![("a/+".into(), 1), ("b/#".into(), 2)],
                }),
                v,
            );
            roundtrip(
                &Packet::SubAck(SubAck {
                    packet_id: 3,
                    properties: p(&props),
                    codes: vec![1, 0x80],
                }),
                v,
            );
            roundtrip(
                &Packet::Unsubscribe(Unsubscribe {
                    packet_id: 4,
                    properties: p(&props),
                    filters: vec!["a/+".into()],
                }),
                v,
            );
            roundtrip(
                &Packet::UnsubAck(SubAck {
                    packet_id: 4,
                    properties: p(&props),
                    codes: if v.is_v5() { vec![0] } else { Vec::new() },
                }),
                v,
            );
            roundtrip(&Packet::PingReq, v);
            roundtrip(&Packet::PingResp, v);
            let d = if v.is_v5() {
                ReasonOnly {
                    reason: Some(0x8E),
                    properties: props.clone(),
                }
            } else {
                ReasonOnly::default()
            };
            roundtrip(&Packet::Disconnect(d.clone()), v);
            if v.is_v5() {
                roundtrip(&Packet::Auth(d), v);
            }
        }
    }

    #[test]
    fn connect_bytes_match_spec_example() {
        // 3.1.1 CONNECT, client id "a", clean session, keep alive 60.
        let bytes =
            Packet::Connect(Box::new(Connect::new(Version::V311, "a"))).encode(Version::V311);
        assert_eq!(
            bytes,
            [
                0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 0x02, 0, 60, 0, 1, b'a'
            ]
        );
    }

    #[test]
    fn decoder_rejects_bad_server_packets() {
        // PINGRESP with flags, CONNACK with trailing bytes, PUBLISH QoS 3, type 0.
        assert!(decode(&[0xD1, 0x00], Version::V311).is_err());
        assert!(decode(&[0x20, 0x03, 0, 0, 0], Version::V311).is_err());
        assert!(decode(&[0x36, 0x03, 0, 1, b'a'], Version::V311).is_err());
        assert!(decode(&[0x00, 0x00], Version::V311).is_err());
        assert!(decode(&[0xF0, 0x00], Version::V311).is_err());
        // 5.0 CONNACK without properties (answer to an unsupported level) is tolerated.
        let (p, _) = decode(&[0x20, 0x02, 0x00, 0x84], Version::V5)
            .unwrap()
            .unwrap();
        assert_eq!(
            p,
            Packet::ConnAck(ConnAck {
                flags: 0,
                code: 0x84,
                properties: Vec::new()
            })
        );
    }
}
