// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Interoperability scenarios: end-to-end client workflows (publish/subscribe at every QoS,
//! retained messages, wills, persistent sessions, keep-alive, ...) that exercise a broker the way
//! real applications do. Written from behavioural descriptions only; no third-party code.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use crate::outcome::{note, skipped};
use crate::{
    Ack, Broker, Client, ConnAck, Connect, Event, HarnessError, Packet, PropValue, Property,
    Publish, ReasonOnly, Will, broker_or_skip, default_timeout, find_prop, prop, quiet_period,
    unique_id,
};

use super::{CONNACK_CODES, SUBACK_FAILURE_CODES, V, connack_u32, props_of};

/// Subscription Options bit: No Local.
const NO_LOCAL: u8 = 0x04;
/// Subscription Options bit: Retain As Published.
const RETAIN_AS_PUBLISHED: u8 = 0x08;
/// Subscription Options: Retain Handling 1 (only for a new subscription).
const RETAIN_HANDLING_NEW: u8 = 0x10;
/// Subscription Options: Retain Handling 2 (never).
const RETAIN_HANDLING_NEVER: u8 = 0x20;
/// The long Session Expiry Interval the scenarios use for persistent sessions.
const LONG_EXPIRY: u32 = 99_999;

/// The per-case topic tree. Everything lives under the unique prefix `p`.
struct Topics {
    p: String,
    /// `P/a` (one level below P).
    t0: String,
    /// `P/a/x`.
    t1: String,
    /// `P/b/c`.
    t2: String,
    /// `P/a/c`.
    t3: String,
    /// `P//a` (an empty level directly below P).
    t4: String,
    /// `P/s`, the shared-subscription topic; no other filter here matches it.
    ts: String,
    /// `P/a/+` (matches T1 and T3).
    w_n1_plus: String,
    /// `P/+/+` (matches T1, T2, T3 and T4).
    w_plus_plus: String,
    /// `P/a/#` (matches T0, T1 and T3); also T0 followed by `/#`.
    w_n1_hash: String,
}

/// A fresh topic tree under a unique prefix. The prefix is short so that a
/// PUBLISH on T0 with a 32-byte payload fits in 64 bytes.
fn topics() -> Topics {
    let p = format!("i5/{}", unique_id("p"));
    Topics {
        t0: format!("{p}/a"),
        t1: format!("{p}/a/x"),
        t2: format!("{p}/b/c"),
        t3: format!("{p}/a/c"),
        t4: format!("{p}//a"),
        ts: format!("{p}/s"),
        w_n1_plus: format!("{p}/a/+"),
        w_plus_plus: format!("{p}/+/+"),
        w_n1_hash: format!("{p}/a/#"),
        p,
    }
}

/// The default client CONNECT: Clean Start 1, Keep Alive 0, no properties.
fn connect(id: &str) -> Connect {
    Connect::new(V, id).keep_alive(0)
}

/// Connects and requires CONNACK 0x00.
fn open(broker: &Broker, connect: &Connect) -> (Client, ConnAck) {
    broker
        .connect_ok(connect)
        .unwrap_or_else(|e| panic!("CONNECT {:?}: {e}", connect.client_id))
}

/// The CONNACK of a throwaway connection, to read the broker's capabilities.
fn probe(broker: &Broker) -> ConnAck {
    let (c, ack) = open(broker, &connect(&unique_id("probe")));
    bye(c);
    ack
}

/// The broker's Maximum QoS (2 when absent).
fn max_qos(ack: &ConnAck) -> u8 {
    u8::try_from(connack_u32(ack, prop::MAXIMUM_QOS, 2)).unwrap_or(2)
}

/// Skips (returning false) unless the broker accepts QoS 2.
fn need_qos2(ack: &ConnAck) -> bool {
    let q = max_qos(ack);
    if q < 2 {
        skipped(format!(
            "broker announces Maximum QoS {q}; the scenario needs QoS 2"
        ));
        return false;
    }
    true
}

/// Skips (returning false) unless the broker supports retained messages.
fn need_retain(ack: &ConnAck) -> bool {
    if connack_u32(ack, prop::RETAIN_AVAILABLE, 1) == 0 {
        skipped("broker announces Retain Available = 0");
        return false;
    }
    true
}

/// Skips (returning false) unless the broker supports wildcard filters.
fn need_wildcards(ack: &ConnAck) -> bool {
    if connack_u32(ack, prop::WILDCARD_SUBSCRIPTION_AVAILABLE, 1) == 0 {
        skipped("broker announces Wildcard Subscription Available = 0");
        return false;
    }
    true
}

/// Skips (returning false) when the broker shortened the requested Session
/// Expiry Interval below `needed`.
fn need_expiry(ack: &ConnAck, requested: u32, needed: u32) -> bool {
    let granted = connack_u32(ack, prop::SESSION_EXPIRY_INTERVAL, requested);
    if granted < needed {
        skipped(format!(
            "broker reduced Session Expiry Interval {requested} to {granted}; the scenario needs {needed}"
        ));
        return false;
    }
    true
}

/// The effective keep-alive after CONNACK (Server Keep Alive overrides the
/// requested value). Skips (returning `None`) when it is 0 or too long for
/// the scenario's timing.
fn effective_keep_alive(ack: &ConnAck, requested: u16) -> Option<Duration> {
    let k = connack_u32(ack, prop::SERVER_KEEP_ALIVE, u32::from(requested));
    if k == 0 {
        skipped("broker announces Server Keep Alive 0 (keep-alive disabled)");
        return None;
    }
    if k > 10 {
        skipped(format!(
            "broker announces Server Keep Alive {k} s, too long for this scenario"
        ));
        return None;
    }
    Some(Duration::from_secs(u64::from(k)))
}

/// Subscribes to one filter and requires success; returns the granted QoS.
fn subscribe_ok(c: &mut Client, filter: &str, options: u8) -> u8 {
    let ack = c
        .subscribe(filter, options)
        .unwrap_or_else(|e| panic!("SUBSCRIBE {filter}: {e}"));
    assert_eq!(ack.codes.len(), 1, "{ack:?}");
    assert!(ack.codes[0] < 0x80, "SUBSCRIBE {filter} refused: {ack:?}");
    ack.codes[0]
}

/// Subscribes asking for QoS 2 (plus `extra` option bits). Skips (returning
/// false) when the broker grants less, since the scenario compares delivered
/// QoS values with the published ones.
fn subscribe_q2(c: &mut Client, filter: &str, extra: u8) -> bool {
    let granted = subscribe_ok(c, filter, 2 | extra);
    if granted < 2 {
        skipped(format!("broker granted QoS {granted} for {filter}"));
        return false;
    }
    true
}

/// Two distinct User Properties, in the order they are sent.
fn user_properties() -> Vec<Property> {
    vec![
        Property::new(
            prop::USER_PROPERTY,
            PropValue::Pair("interop".into(), "first".into()),
        ),
        Property::new(
            prop::USER_PROPERTY,
            PropValue::Pair("trace".into(), "second".into()),
        ),
    ]
}

/// The User Property values of [`user_properties`], in order.
fn user_property_values() -> Vec<PropValue> {
    props_of(&user_properties(), prop::USER_PROPERTY)
}

/// Publishes and completes the flow; any acknowledgement must succeed.
fn publish_with(
    c: &mut Client,
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
    properties: &[Property],
) {
    let mut p = Publish::new(topic, payload);
    p.qos = qos;
    p.retain = retain;
    p.properties = properties.to_vec();
    let ack = c
        .publish_packet(p)
        .unwrap_or_else(|e| panic!("PUBLISH to {topic:?} at QoS {qos}: {e}"));
    if let Some(a) = ack {
        assert!(
            a.reason.unwrap_or(0) < 0x80,
            "PUBLISH to {topic:?} at QoS {qos} refused: {a:?}"
        );
    }
}

/// Receives (and acknowledges) PUBLISH packets until `n` arrived or
/// `timeout` passed, then keeps collecting for the quiet period so that
/// extra copies show up.
fn receive(c: &mut Client, n: usize, timeout: Duration) -> Vec<Publish> {
    let deadline = Instant::now() + timeout;
    let mut out = Vec::new();
    while out.len() < n {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        match c.expect_publish(left) {
            Ok(p) => out.push(p),
            Err(HarnessError::Timeout(_)) => break,
            Err(e) => panic!("while receiving: {e}"),
        }
    }
    out.extend(
        c.collect_publishes(quiet_period())
            .unwrap_or_else(|e| panic!("while receiving: {e}")),
    );
    out
}

/// The delivered QoS values.
fn qos_set(got: &[Publish]) -> BTreeSet<u8> {
    got.iter().map(|p| p.qos).collect()
}

/// The Subscription Identifiers of a delivered PUBLISH, in order.
fn subscription_ids(p: &Publish) -> Vec<u32> {
    props_of(&p.properties, prop::SUBSCRIPTION_IDENTIFIER)
        .into_iter()
        .map(|v| match v {
            PropValue::VarInt(n) => n,
            other => panic!("Subscription Identifier of type {other:?}"),
        })
        .collect()
}

/// Normal DISCONNECT (0x00), then waits (bounded) for the broker to close,
/// so that the next connection with the same Client Identifier finds the
/// session state settled.
fn bye(c: Client) {
    bye_with(c, &[]);
}

/// [`bye`] with DISCONNECT properties.
fn bye_with(mut c: Client, properties: &[Property]) {
    c.send(&Packet::Disconnect(ReasonOnly {
        reason: Some(0),
        properties: properties.to_vec(),
    }))
    .unwrap_or_else(|e| panic!("DISCONNECT: {e}"));
    let _ = c.expect_closed(default_timeout());
}

/// Ends the session of `id` (Clean Start 1, Session Expiry 0, DISCONNECT).
/// Never panics: it runs from [`Cleanup`]'s `drop`.
fn end_session(broker: &Broker, id: &str) {
    if let Ok((mut c, _)) = broker.connect_ok(&connect(id)) {
        let _ = c.send(&Packet::Disconnect(ReasonOnly {
            reason: Some(0),
            properties: Vec::new(),
        }));
        let _ = c.expect_closed(default_timeout());
    }
}

/// The "clear retained" helper, restricted to `prefix`: subscribes to
/// `prefix/#`, and for every retained message received publishes a
/// zero-length retained message to its topic. Never panics.
fn clear_retained(broker: &Broker, prefix: &str) {
    let Ok((mut c, _)) = broker.connect_ok(&connect(&unique_id("clr"))) else {
        return;
    };
    let filter = format!("{prefix}/#");
    if c.subscribe(&filter, 0).is_err() {
        return;
    }
    let Ok(got) = c.collect_publishes(quiet_period()) else {
        return;
    };
    let _ = c.unsubscribe(&filter);
    let retained: BTreeSet<String> = got
        .into_iter()
        .filter(|p| p.retain && !p.payload.is_empty())
        .map(|p| p.topic)
        .collect();
    for t in retained {
        let _ = c.publish(&t, b"", 0, true);
    }
    // PINGRESP comes after every earlier packet of this connection is processed.
    let _ = c.ping();
    let _ = c.send(&Packet::Disconnect(ReasonOnly {
        reason: Some(0),
        properties: Vec::new(),
    }));
    let _ = c.expect_closed(default_timeout());
}

/// Cleans up long-lived broker state when the case ends, also when it fails:
/// ends the listed sessions and clears retained messages under a prefix.
/// Declare it before the clients so it runs after they are dropped.
struct Cleanup<'a> {
    broker: &'a Broker,
    sessions: Vec<String>,
    retained: Option<String>,
}

impl<'a> Cleanup<'a> {
    fn new(broker: &'a Broker) -> Self {
        Self {
            broker,
            sessions: Vec::new(),
            retained: None,
        }
    }

    fn session(mut self, id: &str) -> Self {
        self.sessions.push(id.to_owned());
        self
    }

    fn retained(mut self, prefix: &str) -> Self {
        self.retained = Some(prefix.to_owned());
        self
    }
}

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        for id in &self.sessions {
            end_session(self.broker, id);
        }
        if let Some(p) = &self.retained {
            clear_retained(self.broker, p);
        }
    }
}

/// Connect/disconnect, then self-delivery at QoS 0, 1 and 2 on one topic;
/// a second CONNECT on the same connection must be a Protocol Error (no
/// second CONNACK, connection closed); a CONNECT with a foreign Protocol
/// Name must not succeed.
pub fn connect_publish_subscribe_roundtrip() {
    let broker = broker_or_skip!();
    if !need_qos2(&probe(&broker)) {
        return;
    }
    let t = topics();
    let a = unique_id("a5");
    let (c, _) = open(&broker, &connect(&a));
    bye(c);
    let (mut c, ack) = open(&broker, &connect(&a));
    // req: 5.0/MQTT-3.1.4-5
    assert_eq!(ack.code, 0, "{ack:?}");
    if !subscribe_q2(&mut c, &t.t0, 0) {
        return;
    }
    for qos in 0..=2u8 {
        publish_with(&mut c, &t.t0, &[qos], qos, false, &[]);
    }
    let got = receive(&mut c, 3, default_timeout());
    assert_eq!(got.len(), 3, "deliveries: {got:?}");
    // req: 5.0/MQTT-3.8.4-8
    assert_eq!(qos_set(&got), BTreeSet::from([0, 1, 2]), "{got:?}");
    bye(c);

    // A second CONNECT on an established connection.
    let (mut c, _) = open(&broker, &connect(&a));
    c.send(&Packet::Connect(Box::new(connect(&a))))
        .unwrap_or_else(|e| panic!("second CONNECT: {e}"));
    // req: 5.0/MQTT-3.1.0-2
    let reason = c
        .expect_error_close(default_timeout())
        .unwrap_or_else(|e| panic!("second CONNECT must close the connection: {e}"));
    if let Some(r) = reason.filter(|r| *r != 0x82) {
        note(format!(
            "second CONNECT: DISCONNECT {r:#04x} (0x82 expected)"
        ));
    }

    // A foreign Protocol Name.
    let mut c = broker.open(V).unwrap();
    c.send(&Packet::Connect(Box::new(connect(&a).protocol("MQTX", 5))))
        .unwrap_or_else(|e| panic!("CONNECT with protocol name MQTX: {e}"));
    // req: 5.0/MQTT-3.1.2-1
    let refusal = c
        .expect_refused(default_timeout())
        .unwrap_or_else(|e| panic!("CONNECT with protocol name MQTX: {e}"));
    note(match refusal {
        Some(ack) => format!(
            "foreign protocol name: CONNACK {:#04x}, then close",
            ack.code
        ),
        None => "foreign protocol name: closed without CONNACK".to_owned(),
    });
}

/// Retained messages at QoS 0, 1 and 2, each with two User Properties, are
/// delivered to a later wildcard subscriber at their own QoS with the User
/// Properties intact and in their original order.
pub fn retained_messages_keep_user_properties() {
    let broker = broker_or_skip!();
    let caps = probe(&broker);
    if !need_qos2(&caps) || !need_retain(&caps) || !need_wildcards(&caps) {
        return;
    }
    let t = topics();
    let _cleanup = Cleanup::new(&broker).retained(&t.p);
    let (mut c, _) = open(&broker, &connect(&unique_id("a5")));
    let props = user_properties();
    publish_with(&mut c, &t.t1, b"r0", 0, true, &props);
    publish_with(&mut c, &t.t2, b"r1", 1, true, &props);
    // The QoS 2 flow completes after the broker processed the earlier PUBLISHes.
    publish_with(&mut c, &t.t3, b"r2", 2, true, &props);
    if !subscribe_q2(&mut c, &t.w_plus_plus, 0) {
        return;
    }
    let got = receive(&mut c, 3, default_timeout());
    // req: 5.0/MQTT-3.3.1-5
    assert_eq!(got.len(), 3, "retained deliveries: {got:?}");
    assert_eq!(qos_set(&got), BTreeSet::from([0, 1, 2]), "{got:?}");
    for p in &got {
        // req: 5.0/MQTT-3.3.2-17
        // req: 5.0/MQTT-3.3.2-18
        assert_eq!(
            props_of(&p.properties, prop::USER_PROPERTY),
            user_property_values(),
            "User Properties of the retained message on {}",
            p.topic
        );
    }
    bye(c);
}

/// When keep-alive expires, the Will (Will Delay 0, two User Properties) is
/// published at once, carrying exactly those User Properties in order.
pub fn will_on_keep_alive_timeout_with_properties() {
    let broker = broker_or_skip!();
    let caps = probe(&broker);
    let t = topics();
    let mut will = Will::new(&t.t2, b"keep-alive will");
    will.qos = max_qos(&caps).min(2);
    will.properties = vec![Property::new(
        prop::WILL_DELAY_INTERVAL,
        PropValue::FourByte(0),
    )];
    will.properties.extend(user_properties());
    let (mut ca, ack) = open(&broker, &connect(&unique_id("a5")).keep_alive(2).will(will));
    let start = Instant::now();
    let Some(ka) = effective_keep_alive(&ack, 2) else {
        return;
    };
    let (mut cb, _) = open(&broker, &connect(&unique_id("b5")).clean(false));
    subscribe_ok(&mut cb, &t.t2, 2);
    // Client A stays silent; the broker must close it within 1.5 x keep-alive.
    let limit = ka.mul_f32(1.5) + default_timeout();
    // req: 5.0/MQTT-3.1.2-22
    ca.expect_error_close(limit.saturating_sub(start.elapsed()))
        .unwrap_or_else(|e| panic!("keep-alive {ka:?} not enforced: {e}"));
    let window = Duration::from_secs(10).saturating_sub(start.elapsed());
    let got = receive(&mut cb, 1, window.max(default_timeout()));
    // req: 5.0/MQTT-3.1.2-8
    assert_eq!(got.len(), 1, "expected exactly the Will: {got:?}");
    assert_eq!(got[0].topic, t.t2);
    assert_eq!(got[0].payload, b"keep-alive will");
    // req: 5.0/MQTT-3.1.3-10
    assert_eq!(
        props_of(&got[0].properties, prop::USER_PROPERTY),
        user_property_values(),
        "Will User Properties"
    );
    bye(cb);
}

/// Connects with a zero-length Client Identifier, with Clean Start 0 and
/// then 1. Returns `None` (after recording a skip) when the broker refuses
/// zero-length identifiers; the refusal must be well formed.
fn connect_empty_id(broker: &Broker, clean: bool) -> Option<(Client, ConnAck)> {
    let mut c = broker.open(V).unwrap();
    c.send(&Packet::Connect(Box::new(connect("").clean(clean))))
        .unwrap();
    match c.next_event(default_timeout()).unwrap() {
        Event::Packet(Packet::ConnAck(ack)) if ack.code == 0 => Some((c, ack)),
        Event::Packet(Packet::ConnAck(ack)) => {
            assert!(
                ack.code >= 0x80 && CONNACK_CODES.contains(&ack.code),
                "{ack:?}"
            );
            // req: 5.0/MQTT-3.1.3-8
            c.expect_closed(default_timeout())
                .unwrap_or_else(|e| panic!("after CONNACK {:#04x}: {e}", ack.code));
            skipped(format!(
                "broker refuses a zero-length Client Identifier (Clean Start {}): CONNACK {:#04x}",
                u8::from(clean),
                ack.code
            ));
            None
        }
        Event::Closed => {
            skipped(format!(
                "broker refuses a zero-length Client Identifier (Clean Start {}): closed",
                u8::from(clean)
            ));
            None
        }
        other => panic!("CONNECT with an empty Client Identifier: {other:?}"),
    }
}

/// A zero-length Client Identifier is accepted with Clean Start 0 and with
/// Clean Start 1 (the default configuration permits it).
pub fn empty_client_id_accepted_either_clean_start() {
    let broker = broker_or_skip!();
    for clean in [false, true] {
        let Some((c, ack)) = connect_empty_id(&broker, clean) else {
            return;
        };
        // A newly assigned identifier has no session.
        // req: 5.0/MQTT-3.2.2-3
        // req: 5.0/MQTT-3.2.2-2
        assert!(!ack.session_present(), "Clean Start {clean}: {ack:?}");
        bye(c);
    }
}

/// QoS 1 and 2 messages matching an offline, unexpired session are queued
/// and delivered on reconnect (Session Present 1); QoS 0 queueing is
/// optional and reported.
pub fn offline_queue_for_expiring_session() {
    let broker = broker_or_skip!();
    let caps = probe(&broker);
    if !need_qos2(&caps) || !need_wildcards(&caps) {
        return;
    }
    let t = topics();
    let a = unique_id("a5");
    let _cleanup = Cleanup::new(&broker).session(&a);
    let (mut ca, ack) = open(
        &broker,
        &connect(&a).property(
            prop::SESSION_EXPIRY_INTERVAL,
            PropValue::FourByte(LONG_EXPIRY),
        ),
    );
    if !need_expiry(&ack, LONG_EXPIRY, 60) || !subscribe_q2(&mut ca, &t.w_plus_plus, 0) {
        return;
    }
    bye(ca);
    let (mut cb, _) = open(&broker, &connect(&unique_id("b5")));
    publish_with(&mut cb, &t.t1, b"q0", 0, false, &[]);
    publish_with(&mut cb, &t.t2, b"q1", 1, false, &[]);
    publish_with(&mut cb, &t.t3, b"q2", 2, false, &[]);
    bye(cb);
    let (mut ca, ack) = open(&broker, &connect(&a).clean(false));
    // req: 5.0/MQTT-3.2.2-3
    // req: 5.0/MQTT-3.1.2-23
    assert!(ack.session_present(), "session lost: {ack:?}");
    let got = receive(&mut ca, 3, default_timeout());
    let by_topic: BTreeMap<&str, u8> = got.iter().map(|p| (p.topic.as_str(), p.qos)).collect();
    // req: 5.0/MQTT-3.1.2-5
    // req: 5.0/MQTT-4.5.0-1
    assert_eq!(by_topic.get(t.t2.as_str()), Some(&1), "{got:?}");
    assert_eq!(by_topic.get(t.t3.as_str()), Some(&2), "{got:?}");
    match got.len() {
        2 => note("QoS 0 message was not queued for the offline session"),
        3 => {
            assert_eq!(by_topic.get(t.t1.as_str()), Some(&0), "{got:?}");
            note("QoS 0 message was queued for the offline session");
        }
        _ => panic!("expected 2 or 3 queued messages: {got:?}"),
    }
    bye(ca);
}

/// Two overlapping subscriptions of one client (QoS 2 and QoS 1) in one
/// SUBSCRIBE: a QoS 2 message arrives either once at QoS 2, or once per
/// subscription at that subscription's QoS.
pub fn overlapping_filters_single_or_per_subscription() {
    let broker = broker_or_skip!();
    let caps = probe(&broker);
    if !need_qos2(&caps) || !need_wildcards(&caps) {
        return;
    }
    let t = topics();
    let (mut c, _) = open(&broker, &connect(&unique_id("a5")));
    let ack = c
        .subscribe_many(
            &[(t.w_n1_hash.as_str(), 2), (t.w_n1_plus.as_str(), 1)],
            Vec::new(),
        )
        .unwrap();
    assert_eq!(ack.codes.len(), 2, "{ack:?}");
    assert!(ack.codes.iter().all(|code| *code < 0x80), "{ack:?}");
    let (g_hash, g_plus) = (ack.codes[0], ack.codes[1]);
    publish_with(&mut c, &t.t3, b"overlap", 2, false, &[]);
    let got = receive(&mut c, 1, default_timeout());
    let mut delivered: Vec<u8> = got.iter().map(|p| p.qos).collect();
    delivered.sort_unstable();
    let single = vec![g_hash.max(g_plus)];
    let mut per_subscription = vec![g_hash, g_plus];
    per_subscription.sort_unstable();
    // req: 5.0/MQTT-3.3.4-2
    assert!(
        delivered == single || delivered == per_subscription,
        "granted {ack:?}, delivered QoS {delivered:?}"
    );
    note(if delivered.len() == 1 {
        "overlapping subscriptions: one copy at the highest QoS"
    } else {
        "overlapping subscriptions: one copy per subscription"
    });
    bye(c);
}

/// The broker enforces a 5 s keep-alive (closing within 1.5 x) and then
/// publishes the Will to a subscriber.
pub fn keep_alive_expiry_publishes_will() {
    let broker = broker_or_skip!();
    let caps = probe(&broker);
    let t = topics();
    let mut will = Will::new(&t.t4, b"keep-alive will");
    will.qos = max_qos(&caps).min(2);
    let (mut ca, ack) = open(&broker, &connect(&unique_id("a5")).keep_alive(5).will(will));
    let start = Instant::now();
    let Some(ka) = effective_keep_alive(&ack, 5) else {
        return;
    };
    let (mut cb, _) = open(&broker, &connect(&unique_id("b5")));
    subscribe_ok(&mut cb, &t.t4, 2);
    let limit = ka.mul_f32(1.5) + default_timeout();
    // req: 5.0/MQTT-3.1.2-22
    ca.expect_error_close(limit.saturating_sub(start.elapsed()))
        .unwrap_or_else(|e| panic!("keep-alive {ka:?} not enforced: {e}"));
    note(format!(
        "keep-alive {ka:?}: connection closed {:?} after CONNACK",
        start.elapsed()
    ));
    let got = receive(&mut cb, 1, default_timeout());
    // req: 5.0/MQTT-3.1.2-8
    assert_eq!(got.len(), 1, "expected exactly the Will: {got:?}");
    assert_eq!(got[0].topic, t.t4);
    assert_eq!(got[0].payload, b"keep-alive will");
    bye(cb);
}

/// Unacknowledged inbound QoS 1 and QoS 2 messages are resent when the
/// session resumes: exactly two deliveries after the reconnect, with DUP 1
/// and the original Packet Identifier for those sent before.
pub fn resend_unacknowledged_on_session_resume() {
    let broker = broker_or_skip!();
    let caps = probe(&broker);
    if !need_qos2(&caps) || !need_wildcards(&caps) {
        return;
    }
    let t = topics();
    let b = unique_id("b5");
    let _cleanup = Cleanup::new(&broker).session(&b);
    let persistent = connect(&b).clean(false).property(
        prop::SESSION_EXPIRY_INTERVAL,
        PropValue::FourByte(LONG_EXPIRY),
    );
    let (mut cb, ack) = open(&broker, &persistent);
    if !need_expiry(&ack, LONG_EXPIRY, 60) || !subscribe_q2(&mut cb, &t.w_n1_hash, 0) {
        return;
    }
    // Non-acknowledging mode: own flows complete, inbound PUBLISHes are ignored.
    publish_with(&mut cb, &t.t1, b"", 1, false, &[]);
    publish_with(&mut cb, &t.t3, b"", 2, false, &[]);
    let mut first_sent: HashMap<String, u16> = HashMap::new();
    let deadline = Instant::now() + quiet_period();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        match cb.next_event(left).unwrap() {
            Event::Packet(Packet::Publish(p)) => {
                first_sent.insert(p.topic.clone(), p.packet_id.unwrap_or(0));
            }
            Event::Timeout => break,
            other => panic!("while ignoring deliveries: {other:?}"),
        }
    }
    bye(cb);
    let (mut cb, ack) = open(&broker, &persistent);
    // req: 5.0/MQTT-3.2.2-3
    assert!(ack.session_present(), "session lost: {ack:?}");
    let got = receive(&mut cb, 2, Duration::from_secs(3));
    assert_eq!(got.len(), 2, "redeliveries: {got:?}");
    let topics_seen: BTreeSet<&str> = got.iter().map(|p| p.topic.as_str()).collect();
    // req: 5.0/MQTT-4.4.0-1
    assert_eq!(
        topics_seen,
        BTreeSet::from([t.t1.as_str(), t.t3.as_str()]),
        "{got:?}"
    );
    for p in &got {
        assert_eq!(p.qos, if p.topic == t.t1 { 1 } else { 2 }, "{p:?}");
        if let Some(id) = first_sent.get(&p.topic) {
            // req: 5.0/MQTT-3.3.1-1
            assert!(p.dup, "resent PUBLISH without DUP: {p:?}");
            // req: 5.0/MQTT-4.4.0-1
            assert_eq!(
                p.packet_id,
                Some(*id),
                "resent with a new identifier: {p:?}"
            );
        }
    }
    if first_sent.len() < 2 {
        note(format!(
            "only {} of 2 messages were sent before the disconnect",
            first_sent.len()
        ));
    }
    bye(cb);
}

/// A subscription refused by broker policy gets a SUBACK failure code. The
/// filter comes from `MQTT_TESTKIT_INTEROP_DENY_FILTER`; without it (or when the
/// broker grants it) the case is skipped.
pub fn subscribe_refused_by_policy() {
    let broker = broker_or_skip!();
    let t = topics();
    let filter = crate::env_var("MQTT_TESTKIT_INTEROP_DENY_FILTER")
        .filter(|f| !f.is_empty())
        .unwrap_or_else(|| format!("{}/denied", t.p));
    let (mut c, _) = open(&broker, &connect(&unique_id("a5")));
    let ack = c
        .subscribe(&filter, 2)
        .unwrap_or_else(|e| panic!("SUBSCRIBE {filter}: {e}"));
    assert_eq!(ack.codes.len(), 1, "{ack:?}");
    let code = ack.codes[0];
    if code < 0x80 {
        skipped(format!(
            "broker granted {filter:?} (QoS {code}); set MQTT_TESTKIT_INTEROP_DENY_FILTER to a filter \
             the broker's policy refuses"
        ));
        bye(c);
        return;
    }
    // req: 5.0/MQTT-3.9.3-2
    assert!(
        SUBACK_FAILURE_CODES.contains(&code),
        "undefined SUBACK code: {ack:?}"
    );
    note(format!("{filter:?} refused with {code:#04x}"));
    bye(c);
}

/// Filters that start with a wildcard (`#`, `+/+/+`) do not match a topic
/// that starts with `$`. A separate exact subscriber confirms the `$`
/// message was routed at all, so the check proves something.
pub fn dollar_topics_hidden_from_leading_wildcards() {
    let broker = broker_or_skip!();
    if !need_wildcards(&probe(&broker)) {
        return;
    }
    let dollar = format!("$i5/{}/x", unique_id("d"));
    let (mut watcher, _) = open(&broker, &connect(&unique_id("w5")));
    let ack = watcher.subscribe(&dollar, 1).unwrap();
    if ack.codes[0] >= 0x80 {
        skipped(format!(
            "broker refuses to subscribe to {dollar:?}: {ack:?}"
        ));
        return;
    }
    let (mut cb, _) = open(&broker, &connect(&unique_id("b5")));
    let ack = cb
        .subscribe_many(&[("#", 2), ("+/+/+", 2)], Vec::new())
        .unwrap();
    assert_eq!(ack.codes.len(), 2, "{ack:?}");
    if ack.codes.iter().all(|c| *c >= 0x80) {
        skipped(format!(
            "broker refuses both leading-wildcard filters: {ack:?}"
        ));
        return;
    }
    // Discard whatever matched already (retained messages of other clients).
    cb.collect_publishes(quiet_period()).unwrap();
    let mut p = Publish::new(&dollar, b"");
    p.qos = 1;
    match cb.publish_packet(p) {
        Ok(Some(a)) if a.reason.unwrap_or(0) >= 0x80 => {
            skipped(format!(
                "broker refuses client publishes to $ topics: {a:?}"
            ));
            return;
        }
        Ok(_) => {}
        Err(e) => {
            skipped(format!("broker refuses client publishes to $ topics: {e}"));
            return;
        }
    }
    if receive(&mut watcher, 1, default_timeout()).is_empty() {
        skipped("broker does not deliver client publishes on $ topics");
        return;
    }
    let got = cb.collect_publishes(quiet_period()).unwrap();
    // req: 5.0/MQTT-4.7.2-1
    assert!(
        got.iter().all(|p| p.topic != dollar),
        "a leading-wildcard filter matched {dollar:?}"
    );
    bye(cb);
    bye(watcher);
}

/// After UNSUBSCRIBE from one of three subscriptions, messages on that
/// topic stop while the other two keep arriving.
pub fn unsubscribe_stops_delivery() {
    let broker = broker_or_skip!();
    let t = topics();
    let (mut cb, _) = open(&broker, &connect(&unique_id("b5")));
    for topic in [&t.t0, &t.t1, &t.t2] {
        subscribe_ok(&mut cb, topic, 2);
    }
    let ack = cb.unsubscribe(&t.t0).unwrap();
    // req: 5.0/MQTT-3.10.4-4
    // req: 5.0/MQTT-3.10.4-1
    assert_eq!(ack.codes, [0x00], "{ack:?}");
    cb.collect_publishes(Duration::ZERO).unwrap();
    let (mut ca, _) = open(&broker, &connect(&unique_id("a5")));
    for topic in [&t.t0, &t.t1, &t.t2] {
        publish_with(&mut ca, topic, b"u", 1, false, &[]);
    }
    let got = receive(&mut cb, 2, default_timeout());
    let seen: BTreeSet<&str> = got.iter().map(|p| p.topic.as_str()).collect();
    // req: 5.0/MQTT-3.10.4-2
    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(seen, BTreeSet::from([t.t1.as_str(), t.t2.as_str()]));
    bye(ca);
    bye(cb);
}

/// One connection of the session-expiry lifecycle: connects `id` with the
/// given Clean Start and Session Expiry Interval, optionally subscribes,
/// then disconnects (optionally with a new Session Expiry Interval in
/// DISCONNECT). Returns the CONNACK.
fn lifecycle_step(
    broker: &Broker,
    id: &str,
    clean: bool,
    expiry: u32,
    subscribe: Option<&str>,
    disconnect_expiry: Option<u32>,
) -> ConnAck {
    let (mut c, ack) = open(
        broker,
        &connect(id)
            .clean(clean)
            .property(prop::SESSION_EXPIRY_INTERVAL, PropValue::FourByte(expiry)),
    );
    if let Some(filter) = subscribe {
        subscribe_ok(&mut c, filter, 2);
    }
    let props: Vec<Property> = disconnect_expiry
        .map(|e| Property::new(prop::SESSION_EXPIRY_INTERVAL, PropValue::FourByte(e)))
        .into_iter()
        .collect();
    bye_with(c, &props);
    ack
}

/// Session Expiry Interval semantics: 0 ends the session with the
/// connection, a positive value keeps it that long, and DISCONNECT can
/// change the value (including to 0). Every CONNACK must be 0x00.
pub fn session_expiry_interval_lifecycle() {
    let broker = broker_or_skip!();
    let t = topics();
    let a = unique_id("a5");
    let _cleanup = Cleanup::new(&broker).session(&a);
    let pause = |ms: u64| std::thread::sleep(Duration::from_millis(ms));
    let present = |clean, expiry, sub: Option<&str>, disc| {
        lifecycle_step(&broker, &a, clean, expiry, sub, disc).session_present()
    };

    // req: 5.0/MQTT-3.2.2-2
    assert!(!present(true, 0, Some(&t.t0), None), "step 1");
    // Expiry 0: the session ended with the connection.
    // req: 5.0/MQTT-4.1.0-2
    assert!(!present(false, 0, None, None), "step 2");

    let ack = lifecycle_step(&broker, &a, true, 5, Some(&t.t0), None);
    if !need_expiry(&ack, 5, 5) {
        return;
    }
    // req: 5.0/MQTT-3.2.2-2
    assert!(!ack.session_present(), "step 3: {ack:?}");
    pause(2_000);
    // req: 5.0/MQTT-3.1.2-23
    // req: 5.0/MQTT-3.2.2-3
    assert!(present(false, 5, None, None), "step 4");
    pause(6_500);
    // req: 5.0/MQTT-4.1.0-2
    assert!(!present(false, 5, None, None), "step 5");

    // Expiry 1 s in CONNECT, raised to 5 s by DISCONNECT.
    assert!(!present(true, 1, Some(&t.t0), Some(5)), "step 6");
    pause(3_000);
    // req: 5.0/MQTT-3.1.2-23
    assert!(present(false, 1, None, Some(0)), "step 7");
    // DISCONNECT set the expiry to 0: the session ended.
    // req: 5.0/MQTT-4.1.0-2
    assert!(!present(false, 1, None, None), "step 8");
}

/// User Properties on a PUBLISH reach the subscriber at QoS 0, 1 and 2,
/// both of them and in their original order.
pub fn user_properties_forwarded_at_each_qos() {
    let broker = broker_or_skip!();
    if !need_qos2(&probe(&broker)) {
        return;
    }
    let t = topics();
    let (mut c, _) = open(&broker, &connect(&unique_id("a5")));
    if !subscribe_q2(&mut c, &t.t0, 0) {
        return;
    }
    for qos in 0..=2u8 {
        publish_with(&mut c, &t.t0, b"", qos, false, &user_properties());
    }
    let got = receive(&mut c, 3, Duration::from_secs(5));
    assert_eq!(got.len(), 3, "{got:?}");
    assert_eq!(qos_set(&got), BTreeSet::from([0, 1, 2]), "{got:?}");
    for p in &got {
        // req: 5.0/MQTT-3.3.2-17
        // req: 5.0/MQTT-3.3.2-18
        assert_eq!(
            props_of(&p.properties, prop::USER_PROPERTY),
            user_property_values(),
            "QoS {} delivery",
            p.qos
        );
    }
    bye(c);
}

/// Payload Format Indicator 1 and Content Type are forwarded unchanged at
/// QoS 0, 1 and 2.
pub fn payload_format_and_content_type_forwarded() {
    let broker = broker_or_skip!();
    if !need_qos2(&probe(&broker)) {
        return;
    }
    let t = topics();
    let content_type = "text/plain; charset=utf-8";
    let props = [
        Property::new(prop::PAYLOAD_FORMAT_INDICATOR, PropValue::Byte(1)),
        Property::new(prop::CONTENT_TYPE, PropValue::Str(content_type.into())),
    ];
    let (mut c, _) = open(&broker, &connect(&unique_id("a5")));
    if !subscribe_q2(&mut c, &t.t0, 0) {
        return;
    }
    for qos in 0..=2u8 {
        publish_with(&mut c, &t.t0, b"", qos, false, &props);
    }
    let got = receive(&mut c, 3, Duration::from_secs(5));
    assert_eq!(got.len(), 3, "{got:?}");
    assert_eq!(qos_set(&got), BTreeSet::from([0, 1, 2]), "{got:?}");
    for p in &got {
        // req: 5.0/MQTT-3.3.2-4
        assert_eq!(
            find_prop(&p.properties, prop::PAYLOAD_FORMAT_INDICATOR),
            Some(&PropValue::Byte(1)),
            "{p:?}"
        );
        // req: 5.0/MQTT-3.3.2-20
        assert_eq!(
            find_prop(&p.properties, prop::CONTENT_TYPE),
            Some(&PropValue::Str(content_type.into())),
            "{p:?}"
        );
    }
    bye(c);
}

/// Messages queued for an offline session expire: the 1 s ones are
/// dropped, the 6 s ones arrive with the remaining lifetime (about 3 s).
pub fn message_expiry_while_queued() {
    let broker = broker_or_skip!();
    if !need_qos2(&probe(&broker)) {
        return;
    }
    let t = topics();
    let b = unique_id("b5");
    let _cleanup = Cleanup::new(&broker).session(&b);
    let (mut cb, ack) = open(
        &broker,
        &connect(&b).property(
            prop::SESSION_EXPIRY_INTERVAL,
            PropValue::FourByte(LONG_EXPIRY),
        ),
    );
    if !need_expiry(&ack, LONG_EXPIRY, 60) || !subscribe_q2(&mut cb, &t.t0, 0) {
        return;
    }
    // Keep a reduced expiry if the broker announced one.
    let longer = if find_prop(&ack.properties, prop::SESSION_EXPIRY_INTERVAL).is_some() {
        connack_u32(&ack, prop::SESSION_EXPIRY_INTERVAL, LONG_EXPIRY)
    } else {
        999_999_999
    };
    bye_with(
        cb,
        &[Property::new(
            prop::SESSION_EXPIRY_INTERVAL,
            PropValue::FourByte(longer),
        )],
    );
    let (mut ca, _) = open(&broker, &connect(&unique_id("a5")));
    for (qos, expiry) in [(1u8, 1u32), (2, 1), (1, 6), (2, 6)] {
        let props = [Property::new(
            prop::MESSAGE_EXPIRY_INTERVAL,
            PropValue::FourByte(expiry),
        )];
        publish_with(
            &mut ca,
            &t.t0,
            &[qos, u8::try_from(expiry).unwrap()],
            qos,
            false,
            &props,
        );
    }
    std::thread::sleep(Duration::from_secs(3));
    let (mut cb, ack) = open(&broker, &connect(&b).clean(false));
    assert!(ack.session_present(), "session lost: {ack:?}");
    let got = receive(&mut cb, 2, Duration::from_secs(3));
    let payloads: BTreeSet<Vec<u8>> = got.iter().map(|p| p.payload.clone()).collect();
    // req: 5.0/MQTT-3.3.2-5
    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(
        payloads,
        BTreeSet::from([vec![1, 6], vec![2, 6]]),
        "{got:?}"
    );
    for p in &got {
        let left = match find_prop(&p.properties, prop::MESSAGE_EXPIRY_INTERVAL) {
            Some(PropValue::FourByte(v)) => *v,
            other => panic!("Message Expiry Interval {other:?} on {p:?}"),
        };
        // req: 5.0/MQTT-3.3.2-6
        assert!(
            left < 6,
            "forwarded Message Expiry Interval {left} not reduced"
        );
        note(format!(
            "QoS {} message forwarded with {left} s left",
            p.qos
        ));
    }
    bye(cb);
    bye(ca);
}

/// Part (a) of the subscription-options scenario: No Local.
fn no_local_part(broker: &Broker, t: &Topics) {
    let (mut ca, _) = open(broker, &connect(&unique_id("a5")));
    let (mut cb, _) = open(broker, &connect(&unique_id("b5")));
    subscribe_ok(&mut ca, &t.t0, 2 | NO_LOCAL);
    subscribe_ok(&mut cb, &t.t0, 2 | NO_LOCAL);
    publish_with(&mut ca, &t.t0, b"no-local", 1, false, &[]);
    let at_b = receive(&mut cb, 1, Duration::from_secs(3));
    assert_eq!(at_b.len(), 1, "other subscriber: {at_b:?}");
    let at_a = ca.collect_publishes(quiet_period()).unwrap();
    // req: 5.0/MQTT-3.8.3-3
    assert!(
        at_a.is_empty(),
        "No Local delivered to the publisher: {at_a:?}"
    );
    bye(ca);
    bye(cb);
}

/// Part (b): Retain As Published. Leaves a retained message on T0.
fn retain_as_published_part(broker: &Broker, t: &Topics) {
    let (mut c, _) = open(broker, &connect(&unique_id("a5")));
    subscribe_ok(&mut c, &t.t0, 2 | RETAIN_AS_PUBLISHED);
    publish_with(&mut c, &t.t0, b"plain", 1, false, &[]);
    publish_with(&mut c, &t.t0, b"retained", 1, true, &[]);
    let got = receive(&mut c, 2, Duration::from_secs(3));
    assert_eq!(got.len(), 2, "{got:?}");
    // req: 5.0/MQTT-3.3.1-13
    assert_eq!(
        got.iter()
            .map(|p| (p.payload.as_slice(), p.retain))
            .collect::<Vec<_>>(),
        [(&b"plain"[..], false), (&b"retained"[..], true)]
    );
    bye(c);
}

/// Subscribes to W-plus-plus with `options` and returns what arrives.
fn subscribe_and_collect(c: &mut Client, t: &Topics, options: u8, expect: usize) -> Vec<Publish> {
    subscribe_ok(c, &t.w_plus_plus, options);
    receive(c, expect, default_timeout())
}

/// Parts (c), (d) and (e): Retain Handling 1, 2 and 0.
fn retain_handling_parts(broker: &Broker, t: &Topics) {
    let all = BTreeSet::from([0u8, 1, 2]);
    // (c) Retain Handling 1.
    let (mut c, _) = open(broker, &connect(&unique_id("a5")));
    publish_with(&mut c, &t.t1, b"c0", 0, true, &[]);
    publish_with(&mut c, &t.t2, b"c1", 1, true, &[]);
    publish_with(&mut c, &t.t3, b"c2", 2, true, &[]);
    let first = subscribe_and_collect(&mut c, t, 2 | RETAIN_HANDLING_NEW, 3);
    // req: 5.0/MQTT-3.3.1-10
    assert_eq!(
        first.len(),
        3,
        "Retain Handling 1, new subscription: {first:?}"
    );
    assert_eq!(qos_set(&first), all, "{first:?}");
    let again = subscribe_and_collect(&mut c, t, 2 | RETAIN_HANDLING_NEW, 0);
    // req: 5.0/MQTT-3.3.1-10
    assert!(
        again.is_empty(),
        "Retain Handling 1, existing subscription: {again:?}"
    );
    bye(c);

    // (d) Retain Handling 2.
    let (mut c, _) = open(broker, &connect(&unique_id("a5")));
    for round in ["new", "existing"] {
        let got = subscribe_and_collect(&mut c, t, 2 | RETAIN_HANDLING_NEVER, 0);
        // req: 5.0/MQTT-3.3.1-11
        assert!(
            got.is_empty(),
            "Retain Handling 2, {round} subscription: {got:?}"
        );
    }
    bye(c);

    // (e) Retain Handling 0.
    let (mut c, _) = open(broker, &connect(&unique_id("a5")));
    for round in ["new", "existing"] {
        let got = subscribe_and_collect(&mut c, t, 2, 3);
        // req: 5.0/MQTT-3.3.1-9
        // req: 5.0/MQTT-3.8.4-4
        assert_eq!(
            got.len(),
            3,
            "Retain Handling 0, {round} subscription: {got:?}"
        );
        assert_eq!(qos_set(&got), all, "{got:?}");
    }
    bye(c);
}

/// The three MQTT 5.0 Subscription Options: No Local, Retain As Published
/// and Retain Handling 0, 1 and 2.
pub fn subscription_options_no_local_rap_retain_handling() {
    let broker = broker_or_skip!();
    let caps = probe(&broker);
    if !need_qos2(&caps) || !need_retain(&caps) || !need_wildcards(&caps) {
        return;
    }
    let t = topics();
    let _cleanup = Cleanup::new(&broker).retained(&t.p);
    no_local_part(&broker, &t);
    retain_as_published_part(&broker, &t);
    retain_handling_parts(&broker, &t);
}

/// A zero-length Client Identifier gets a non-empty Assigned Client
/// Identifier in CONNACK.
pub fn assigned_client_identifier_returned() {
    let broker = broker_or_skip!();
    let Some((c, ack)) = connect_empty_id(&broker, true) else {
        return;
    };
    // req: 5.0/MQTT-3.2.2-16
    // req: 5.0/MQTT-3.1.3-7
    match find_prop(&ack.properties, prop::ASSIGNED_CLIENT_IDENTIFIER) {
        Some(PropValue::Str(id)) => assert!(!id.is_empty(), "empty assigned id: {ack:?}"),
        other => panic!("no Assigned Client Identifier: {other:?} in {ack:?}"),
    }
    bye(c);
}

/// Subscription Identifiers come back on delivery: a six-digit identifier
/// (multi-byte Variable Byte Integer) on a single subscription, and both
/// identifiers of two overlapping subscriptions (one copy carrying both, or
/// one copy each).
pub fn subscription_identifiers_returned_on_delivery() {
    let broker = broker_or_skip!();
    let caps = probe(&broker);
    if connack_u32(&caps, prop::SUBSCRIPTION_IDENTIFIER_AVAILABLE, 1) == 0 {
        skipped("broker announces Subscription Identifier Available = 0");
        return;
    }
    if !need_wildcards(&caps) {
        return;
    }
    let t = topics();
    let id = |n: u32| {
        vec![Property::new(
            prop::SUBSCRIPTION_IDENTIFIER,
            PropValue::VarInt(n),
        )]
    };
    let (big, s1, s2) = (123_456u32, 2u32, 3u32);
    let (mut ca, _) = open(&broker, &connect(&unique_id("a5")));
    let ack = ca.subscribe_many(&[(t.t0.as_str(), 2)], id(big)).unwrap();
    assert!(ack.codes[0] < 0x80, "{ack:?}");
    let (mut cb, _) = open(&broker, &connect(&unique_id("b5")));
    for (filter, n) in [(&t.t0, s1), (&t.w_n1_hash, s2)] {
        let ack = cb.subscribe_many(&[(filter.as_str(), 2)], id(n)).unwrap();
        assert!(ack.codes[0] < 0x80, "{ack:?}");
    }
    publish_with(&mut cb, &t.t0, b"ids", 1, false, &[]);
    let at_a = receive(&mut ca, 1, Duration::from_secs(3));
    assert_eq!(at_a.len(), 1, "{at_a:?}");
    // req: 5.0/MQTT-3.3.4-3
    assert_eq!(subscription_ids(&at_a[0]), [big], "{:?}", at_a[0]);
    bye(ca);
    let at_b = receive(&mut cb, 1, Duration::from_secs(3));
    match at_b.as_slice() {
        [p] => {
            let ids: BTreeSet<u32> = subscription_ids(p).into_iter().collect();
            // req: 5.0/MQTT-3.3.4-3
            // req: 5.0/MQTT-3.3.4-4
            assert_eq!(ids, BTreeSet::from([s1, s2]), "{p:?}");
            note("overlapping subscriptions: one copy with both identifiers");
        }
        [p, q] => {
            let mut each = vec![subscription_ids(p), subscription_ids(q)];
            each.sort();
            // req: 5.0/MQTT-3.3.4-3
            // req: 5.0/MQTT-3.3.4-5
            assert_eq!(each, [vec![s1], vec![s2]], "{at_b:?}");
            note("overlapping subscriptions: one copy per subscription");
        }
        other => panic!("expected 1 or 2 copies: {other:?}"),
    }
    bye(cb);
}

/// Request/response: Response Topic and Correlation Data pass through
/// unchanged, and the responder's reply reaches the requester (No Local on
/// both subscriptions keeps each from seeing its own message).
pub fn request_response_properties_roundtrip() {
    let broker = broker_or_skip!();
    let t = topics();
    let correlation = vec![0x00, 0xFF, 0x10, b'c', b'o', b'r', 0x7F];
    let (mut ca, _) = open(&broker, &connect(&unique_id("a5")));
    let (mut cb, _) = open(&broker, &connect(&unique_id("b5")));
    subscribe_ok(&mut ca, &t.t0, 2 | NO_LOCAL);
    subscribe_ok(&mut cb, &t.t0, 2 | NO_LOCAL);
    let request = [
        Property::new(prop::RESPONSE_TOPIC, PropValue::Str(t.t0.clone())),
        Property::new(prop::CORRELATION_DATA, PropValue::Bin(correlation.clone())),
    ];
    publish_with(&mut ca, &t.t0, b"request", 1, false, &request);
    let at_b = receive(&mut cb, 1, Duration::from_secs(3));
    assert_eq!(at_b.len(), 1, "{at_b:?}");
    let response_topic = match find_prop(&at_b[0].properties, prop::RESPONSE_TOPIC) {
        Some(PropValue::Str(s)) => s.clone(),
        other => panic!("Response Topic {other:?} in {:?}", at_b[0]),
    };
    // req: 5.0/MQTT-3.3.2-15
    assert_eq!(response_topic, t.t0);
    let received_correlation = find_prop(&at_b[0].properties, prop::CORRELATION_DATA).cloned();
    // req: 5.0/MQTT-3.3.2-16
    assert_eq!(
        received_correlation,
        Some(PropValue::Bin(correlation.clone()))
    );
    // Echo only Response Topic and Correlation Data (never a Subscription
    // Identifier or Topic Alias).
    let reply = [
        Property::new(prop::RESPONSE_TOPIC, PropValue::Str(response_topic.clone())),
        Property::new(prop::CORRELATION_DATA, PropValue::Bin(correlation.clone())),
    ];
    publish_with(&mut cb, &response_topic, b"response", 1, false, &reply);
    let at_a = receive(&mut ca, 1, Duration::from_secs(3));
    // req: 5.0/MQTT-3.8.3-3
    assert_eq!(at_a.len(), 1, "requester: {at_a:?}");
    assert_eq!(at_a[0].payload, b"response");
    // req: 5.0/MQTT-3.3.2-16
    assert_eq!(
        find_prop(&at_a[0].properties, prop::CORRELATION_DATA),
        Some(&PropValue::Bin(correlation))
    );
    let echo = cb.collect_publishes(Duration::ZERO).unwrap();
    // req: 5.0/MQTT-3.8.3-3
    assert!(echo.is_empty(), "responder got its own response: {echo:?}");
    bye(ca);
    bye(cb);
}

/// A QoS 1 PUBLISH with a Topic Alias property.
fn aliased(topic: &str, alias: u16, packet_id: u16, payload: &[u8]) -> Packet {
    let mut p = Publish::new(topic, payload);
    p.qos = 1;
    p.packet_id = Some(packet_id);
    p.properties = vec![Property::new(prop::TOPIC_ALIAS, PropValue::TwoByte(alias))];
    Packet::Publish(p)
}

/// Topic Aliases sent to the broker: alias 0 is invalid; a valid alias maps
/// the topic for the rest of the connection; mappings do not carry over to
/// a new connection. Part 1's disconnect relies on the broker detecting a
/// client-side violation (strict tier in the main suite), so a broker that
/// accepts alias 0 is noted, not failed; a reaction must be correct.
pub fn inbound_topic_alias_rules() {
    let broker = broker_or_skip!();
    let t = topics();
    let client = unique_id("a5");
    let _cleanup = Cleanup::new(&broker).session(&client);

    // Part 1: alias 0.
    let (mut c, _) = open(&broker, &connect(&client));
    let id = c.next_packet_id();
    c.send(&aliased(&t.t0, 0, id, b"alias 0")).unwrap();
    match c.next_event(default_timeout()).unwrap() {
        Event::Packet(Packet::Disconnect(d)) => {
            let r = d.reason.unwrap_or(0);
            assert!(matches!(r, 0x94 | 0x82), "Topic Alias 0: DISCONNECT {d:?}");
            c.expect_closed(default_timeout())
                .unwrap_or_else(|e| panic!("after DISCONNECT {r:#04x}: {e}"));
        }
        Event::Closed => note("Topic Alias 0: closed without DISCONNECT"),
        other => {
            note(format!(
                "Topic Alias 0 not detected (strict-tier behaviour): {other:?}"
            ));
            bye(c);
        }
    }

    // Part 2: a valid alias within one connection.
    let (mut c, ack) = open(
        &broker,
        &connect(&client)
            .property(prop::TOPIC_ALIAS_MAXIMUM, PropValue::TwoByte(0))
            .property(
                prop::SESSION_EXPIRY_INTERVAL,
                PropValue::FourByte(LONG_EXPIRY),
            ),
    );
    if connack_u32(&ack, prop::TOPIC_ALIAS_MAXIMUM, 0) == 0 {
        note("broker announces Topic Alias Maximum 0: parts 2 and 3 do not apply");
        bye(c);
        return;
    }
    if !need_expiry(&ack, LONG_EXPIRY, 60) {
        return;
    }
    subscribe_ok(&mut c, &t.t0, 2);
    for (topic, payload) in [(t.t0.as_str(), b"set"), ("", b"use")] {
        let id = c.next_packet_id();
        c.send(&aliased(topic, 1, id, payload)).unwrap();
        // req: 5.0/MQTT-3.3.2-12
        let ack = c
            .wait_for(default_timeout(), "PUBACK", |p| match p {
                Packet::PubAck(a) if a.packet_id == id => Some(a.clone()),
                _ => None,
            })
            .unwrap_or_else(|e| panic!("PUBLISH {topic:?} with Topic Alias 1: {e}"));
        assert!(ack.reason.unwrap_or(0) < 0x80, "{ack:?}");
    }
    let got = receive(&mut c, 2, default_timeout());
    assert_eq!(got.len(), 2, "{got:?}");
    for p in &got {
        // req: 5.0/MQTT-3.3.2-12
        assert_eq!(p.topic, t.t0, "{p:?}");
        // req: 5.0/MQTT-3.1.2-27
        assert!(
            find_prop(&p.properties, prop::TOPIC_ALIAS).is_none(),
            "{p:?}"
        );
    }
    bye(c);

    // Part 3: the mapping is gone on a new connection.
    let (mut c, ack) = open(&broker, &connect(&client).clean(false));
    assert!(ack.session_present(), "session lost: {ack:?}");
    publish_with(&mut c, &t.t0, b"plain", 1, false, &[]);
    let got = receive(&mut c, 1, default_timeout());
    assert_eq!(got.len(), 1, "{got:?}");
    let id = c.next_packet_id();
    c.send(&aliased("", 1, id, b"stale alias")).unwrap();
    // req: 5.0/MQTT-3.3.2-7
    // req: 5.0/MQTT-4.13.1-1
    let reason = c
        .expect_error_close(default_timeout())
        .unwrap_or_else(|e| panic!("unmapped Topic Alias on a new connection: {e}"));
    if let Some(r) = reason.filter(|r| !matches!(r, 0x82 | 0x94)) {
        note(format!("unmapped Topic Alias: DISCONNECT {r:#04x}"));
    }
}

/// Self-delivery at QoS 0, 1 and 2 on a connection that declares
/// `alias_max` (absent when `None`).
fn deliveries_with_alias_maximum(
    broker: &Broker,
    t: &Topics,
    alias_max: Option<u16>,
) -> Vec<Publish> {
    let mut conn = connect(&unique_id("a5"));
    if let Some(n) = alias_max {
        conn = conn.property(prop::TOPIC_ALIAS_MAXIMUM, PropValue::TwoByte(n));
    }
    let (mut c, _) = open(broker, &conn);
    subscribe_ok(&mut c, &t.t0, 2);
    for qos in 0..=2u8 {
        publish_with(&mut c, &t.t0, &[qos], qos, false, &[]);
    }
    let got = receive(&mut c, 3, Duration::from_secs(3));
    bye(c);
    assert_eq!(got.len(), 3, "Topic Alias Maximum {alias_max:?}: {got:?}");
    got
}

/// The Topic Alias of a delivered PUBLISH, if any.
fn alias_of(p: &Publish) -> Option<u16> {
    match find_prop(&p.properties, prop::TOPIC_ALIAS) {
        None => None,
        Some(PropValue::TwoByte(a)) => Some(*a),
        Some(other) => panic!("Topic Alias of type {other:?}"),
    }
}

/// The broker uses Topic Aliases towards a client only within the client's
/// Topic Alias Maximum (1 here) and none when it is absent or 0. Using
/// aliases at all is optional; whether the broker did is noted.
pub fn outbound_topic_alias_respects_client_maximum() {
    let broker = broker_or_skip!();
    if !need_qos2(&probe(&broker)) {
        return;
    }
    let t = topics();
    // (a) Topic Alias Maximum 1.
    let got = deliveries_with_alias_maximum(&broker, &t, Some(1));
    let mut mapped: HashMap<u16, String> = HashMap::new();
    for p in &got {
        let Some(a) = alias_of(p) else {
            assert_eq!(p.topic, t.t0, "{p:?}");
            continue;
        };
        // req: 5.0/MQTT-3.1.2-26
        // req: 5.0/MQTT-3.3.2-11
        assert_eq!(a, 1, "Topic Alias above the client's maximum: {p:?}");
        if p.topic.is_empty() {
            assert!(
                mapped.contains_key(&a),
                "alias {a} used before it was set: {p:?}"
            );
        } else {
            mapped.insert(a, p.topic.clone());
        }
        assert_eq!(mapped.get(&a), Some(&t.t0), "{p:?}");
    }
    let aliased_count = got.iter().filter(|p| alias_of(p).is_some()).count();
    let always_aliased = aliased_count == 3 && !got[0].topic.is_empty() && got[1].topic.is_empty();
    note(format!(
        "Topic Alias Maximum 1: {aliased_count} of 3 deliveries used an alias \
         (alias on every delivery after the first: {})",
        if always_aliased { "yes" } else { "no" }
    ));
    // (b) No Topic Alias Maximum; (c) Topic Alias Maximum 0.
    for alias_max in [None, Some(0)] {
        for p in deliveries_with_alias_maximum(&broker, &t, alias_max) {
            // req: 5.0/MQTT-3.1.2-27
            assert_eq!(
                alias_of(&p),
                None,
                "Topic Alias Maximum {alias_max:?}: {p:?}"
            );
            assert_eq!(p.topic, t.t0, "{p:?}");
        }
    }
}

/// Part 1 of the Maximum Packet Size scenario: a PUBLISH larger than the
/// broker's limit. Detection is strict tier in the main suite, so a broker
/// that keeps the connection is noted; a DISCONNECT must carry 0x95.
fn server_limit_part(broker: &Broker, t: &Topics) {
    let (mut c, ack) = open(broker, &connect(&unique_id("a5")));
    let limit = connack_u32(&ack, prop::MAXIMUM_PACKET_SIZE, crate::codec::VARINT_MAX);
    if limit >= 65_535 {
        note(format!(
            "broker Maximum Packet Size {limit}: part 1 (server limit) does not apply"
        ));
        bye(c);
        return;
    }
    let payload = vec![b'x'; usize::try_from(limit).unwrap()];
    // The broker may close while the packet is still being written.
    let _ = c.send(&Packet::Publish(Publish::new(&t.t0, &payload)));
    let deadline = Instant::now() + default_timeout();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match c.next_event(left) {
            Ok(Event::Packet(Packet::Disconnect(d))) => {
                // req: 5.0/MQTT-3.2.2-15
                assert_eq!(d.reason, Some(0x95), "oversized PUBLISH: {d:?}");
            }
            Ok(Event::Closed) | Err(HarnessError::Io(_) | HarnessError::Closed(_)) => return,
            Ok(Event::Timeout) => {
                note(format!(
                    "a PUBLISH above the broker's Maximum Packet Size {limit} was not detected \
                     (strict-tier behaviour)"
                ));
                return;
            }
            Ok(Event::Packet(other)) => panic!("oversized PUBLISH: unexpected {other:?}"),
            Err(e) => panic!("oversized PUBLISH: {e}"),
        }
    }
}

/// The broker enforces its own Maximum Packet Size (if below 65535) and
/// never sends a client a packet above the client's Maximum Packet Size
/// (64 bytes): the oversized message is dropped, the connection stays up
/// and the sender still gets its PUBACK.
pub fn maximum_packet_size_both_directions() {
    let broker = broker_or_skip!();
    let t = topics();
    server_limit_part(&broker, &t);

    // Part 2: the client's limit.
    let limit = 64usize;
    let (mut c, _) = open(
        &broker,
        &connect(&unique_id("a5")).property(prop::MAXIMUM_PACKET_SIZE, PropValue::FourByte(64)),
    );
    subscribe_ok(&mut c, &t.t0, 2);
    // Fixed header (2) + topic length (2) + topic + Property Length (1) + payload.
    let fits = limit.saturating_sub(5 + t.t0.len()).min(32);
    publish_with(&mut c, &t.t0, &vec![b's'; fits], 0, false, &[]);
    let got = receive(&mut c, 1, Duration::from_secs(3));
    assert_eq!(
        got.len(),
        1,
        "a {fits}-byte payload fits in 64 bytes: {got:?}"
    );
    let size = Packet::Publish(got[0].clone()).encode(V).len();
    // req: 5.0/MQTT-3.1.2-24
    assert!(
        size <= limit,
        "{size}-byte PUBLISH sent to a 64-byte client"
    );
    // The sender (this client) still gets its PUBACK.
    publish_with(&mut c, &t.t0, &[b'l'; 64], 1, false, &[]);
    let more = c.collect_publishes(Duration::from_secs(2)).unwrap();
    // req: 5.0/MQTT-3.1.2-24
    // req: 5.0/MQTT-3.1.2-25
    assert!(more.is_empty(), "oversized message sent: {more:?}");
    c.ping()
        .unwrap_or_else(|e| panic!("connection lost after the oversized message: {e}"));
    bye(c);
}

/// A Keep Alive of 120 s is overridden when the broker's configured maximum
/// is lower, and the chosen value is reported in Server Keep Alive. Set
/// `MQTT_TESTKIT_INTEROP_SERVER_KEEP_ALIVE` to the broker's configured maximum
/// to assert it; otherwise the announced value is only recorded.
pub fn server_keep_alive_override() {
    let broker = broker_or_skip!();
    let (c, ack) = open(&broker, &connect(&unique_id("a5")).keep_alive(120));
    let announced = find_prop(&ack.properties, prop::SERVER_KEEP_ALIVE).cloned();
    let configured = crate::env_var("MQTT_TESTKIT_INTEROP_SERVER_KEEP_ALIVE")
        .and_then(|v| v.trim().parse::<u16>().ok());
    match configured {
        Some(k) if k < 120 => assert_eq!(
            announced,
            Some(PropValue::TwoByte(k)),
            "configured maximum keep-alive {k} s"
        ),
        _ => match announced {
            None => note("no Server Keep Alive: the requested 120 s applies"),
            Some(PropValue::TwoByte(120)) => note("Server Keep Alive 120 (unchanged)"),
            Some(PropValue::TwoByte(k)) => note(format!(
                "Server Keep Alive {k} s (set MQTT_TESTKIT_INTEROP_SERVER_KEEP_ALIVE to assert it)"
            )),
            Some(other) => panic!("Server Keep Alive of type {other:?}"),
        },
    }
    bye(c);
}

/// The broker never has more unacknowledged QoS 1 PUBLISH packets in flight
/// to a client than the client's Receive Maximum (2): the third one waits
/// until the first is acknowledged. Raw reads, acknowledgements by hand.
pub fn outbound_flow_control_receive_maximum() {
    let broker = broker_or_skip!();
    let t = topics();
    let (mut c, ack) = open(
        &broker,
        &connect(&unique_id("a5"))
            .property(prop::RECEIVE_MAXIMUM, PropValue::TwoByte(2))
            .property(prop::SESSION_EXPIRY_INTERVAL, PropValue::FourByte(0)),
    );
    if connack_u32(&ack, prop::RECEIVE_MAXIMUM, 65_535) < 3 {
        skipped("broker Receive Maximum below 3");
        return;
    }
    if max_qos(&ack) < 1 {
        skipped("broker announces Maximum QoS 0");
        return;
    }
    subscribe_ok(&mut c, &t.t0, 2);
    let mut ids = Vec::new();
    for i in 0..3u8 {
        let id = c.next_packet_id();
        let mut p = Publish::new(&t.t0, &[i]);
        p.qos = 1;
        p.packet_id = Some(id);
        c.send(&Packet::Publish(p)).unwrap();
        ids.push(id);
    }
    let mut acked = BTreeSet::new();
    let mut inbound: Vec<Publish> = Vec::new();
    let deadline = Instant::now() + default_timeout();
    while acked.len() < 3 || inbound.len() < 2 {
        let left = deadline.saturating_duration_since(Instant::now());
        match c.next_event(left).unwrap() {
            Event::Packet(Packet::PubAck(a)) => {
                assert!(
                    ids.contains(&a.packet_id) && acked.insert(a.packet_id),
                    "{a:?}"
                );
            }
            Event::Packet(Packet::Publish(p)) => {
                assert_eq!(p.qos, 1, "{p:?}");
                // req: 5.0/MQTT-3.3.4-9
                assert!(
                    inbound.len() < 2,
                    "third PUBLISH with two unacknowledged: {p:?}"
                );
                inbound.push(p);
            }
            other => panic!(
                "expected PUBACKs and 2 PUBLISHes, got {other:?} (PUBACKs {acked:?}, PUBLISHes {})",
                inbound.len()
            ),
        }
    }
    // req: 5.0/MQTT-3.3.4-9
    c.expect_no_packet(quiet_period())
        .unwrap_or_else(|e| panic!("with 2 unacknowledged PUBLISHes: {e}"));
    c.send(&Packet::PubAck(Ack::new(inbound[0].packet_id.unwrap())))
        .unwrap();
    let third = c
        .expect_publish_unacked(default_timeout())
        .unwrap_or_else(|e| panic!("third PUBLISH after a PUBACK: {e}"));
    assert_eq!(third.qos, 1, "{third:?}");
    let payloads: BTreeSet<u8> = inbound
        .iter()
        .chain([&third])
        .map(|p| p.payload[0])
        .collect();
    assert_eq!(payloads, BTreeSet::from([0, 1, 2]));
    for p in [&inbound[1], &third] {
        c.send(&Packet::PubAck(Ack::new(p.packet_id.unwrap())))
            .unwrap();
    }
    bye(c);
}

/// Strict: MQTT-3.3.4-7 binds the client; the 0x93 DISCONNECT relies on the server detecting the excess.
/// A client that keeps more QoS 2 PUBLISH packets unacknowledged than the
/// broker's Receive Maximum is disconnected with 0x93. Runs only when the
/// broker announces a Receive Maximum of at most 65000.
pub fn inbound_receive_maximum_exceeded() {
    let broker = broker_or_skip!();
    let t = topics();
    let (mut c, ack) = open(&broker, &connect(&unique_id("a5")));
    let max = connack_u32(&ack, prop::RECEIVE_MAXIMUM, 65_535);
    if max > 65_000 {
        skipped(format!(
            "broker Receive Maximum {max}: too few free packet identifiers to exceed it"
        ));
        return;
    }
    if max_qos(&ack) < 2 {
        skipped("broker announces Maximum QoS below 2");
        return;
    }
    let qos2 = |id: u32| {
        let mut p = Publish::new(&t.t0, b"");
        p.qos = 2;
        p.packet_id = Some(u16::try_from(id).unwrap());
        Packet::Publish(p)
    };
    // `max` QoS 2 flows that never get a PUBREL, so all stay in flight.
    // PUBRECs are read per batch so that neither side blocks on a full buffer.
    let mut next = 1u32;
    while next <= max {
        let end = (next + 500).min(max + 1);
        for id in next..end {
            c.send(&qos2(id)).unwrap();
        }
        for _ in next..end {
            match c.expect_packet(default_timeout()).unwrap() {
                Packet::PubRec(a) => assert!(a.reason.unwrap_or(0) < 0x80, "{a:?}"),
                other => panic!("within the Receive Maximum: {other:?}"),
            }
        }
        next = end;
    }
    let extra = max + 1;
    c.send(&qos2(extra)).unwrap();
    let mut disconnects = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match c.next_event(left).unwrap() {
            Event::Packet(Packet::Disconnect(d)) => disconnects.push(d.reason.unwrap_or(0)),
            Event::Packet(Packet::PubRec(a)) if disconnects.is_empty() => {
                // req: 5.0/MQTT-3.3.4-7
                assert!(
                    u32::from(a.packet_id) == extra && a.reason.unwrap_or(0) >= 0x80,
                    "PUBLISH beyond the Receive Maximum accepted: {a:?}"
                );
                note(format!(
                    "excess PUBLISH refused with PUBREC {:#04x}",
                    a.reason.unwrap_or(0)
                ));
                bye(c);
                return;
            }
            Event::Closed => break,
            other => panic!("after exceeding Receive Maximum {max}: {other:?}"),
        }
    }
    // req: 5.0/MQTT-3.3.4-7
    match disconnects.as_slice() {
        [r] => assert_eq!(*r, 0x93, "Receive Maximum exceeded"),
        [] => note("closed without DISCONNECT (0x93 expected if one is sent)"),
        many => panic!("several DISCONNECTs: {many:02x?}"),
    }
}

/// One case of the will-delay scenario: the Will is published at the end of
/// the Will Delay Interval or of the session, whichever comes first.
/// Returns false (after recording a skip) when the broker shortens the
/// session expiry.
fn will_delay_case(broker: &Broker, topic: &str, will_qos: u8, delay: u32, expiry: u32) -> bool {
    let a = unique_id("a5");
    let _cleanup = Cleanup::new(broker).session(&a);
    let payload = format!("will {delay}/{expiry}");
    let mut will = Will::new(topic, payload.as_bytes());
    will.qos = will_qos;
    will.properties = vec![Property::new(
        prop::WILL_DELAY_INTERVAL,
        PropValue::FourByte(delay),
    )];
    let (ca, ack) = open(
        broker,
        &connect(&a)
            .property(prop::SESSION_EXPIRY_INTERVAL, PropValue::FourByte(expiry))
            .will(will),
    );
    assert!(!ack.session_present(), "{ack:?}");
    if !need_expiry(&ack, expiry, expiry) {
        return false;
    }
    let (mut cb, _) = open(broker, &connect(&unique_id("b5")));
    subscribe_ok(&mut cb, topic, 2);
    ca.abort();
    let closed = Instant::now();
    let got = cb
        .expect_publish(Duration::from_secs(10))
        .unwrap_or_else(|e| panic!("Will Delay {delay} s, Session Expiry {expiry} s: {e}"));
    let elapsed = closed.elapsed();
    let nominal = Duration::from_secs(u64::from(delay.min(expiry)));
    note(format!(
        "Will Delay {delay} s, Session Expiry {expiry} s: Will after {elapsed:.2?} (nominal {nominal:?})"
    ));
    assert_eq!(got.topic, topic);
    assert_eq!(got.payload, payload.as_bytes());
    // req: 5.0/MQTT-3.1.2-8
    assert!(
        elapsed + Duration::from_millis(200) >= nominal
            && elapsed <= nominal + Duration::from_millis(1_500),
        "Will Delay {delay} s, Session Expiry {expiry} s: Will after {elapsed:?}, expected \
         about {nominal:?}"
    );
    let extra = cb.collect_publishes(quiet_period()).unwrap();
    assert!(extra.is_empty(), "more than one Will: {extra:?}");
    bye(cb);
    true
}

/// The Will is published when the Will Delay Interval or the session ends,
/// whichever is first: delay 3 / expiry 5 (about 3 s), delay 5 / expiry 0
/// (at once), delay 5 / expiry 2 (about 2 s). Window: nominal - 0.2 s to
/// nominal + 1.5 s.
pub fn will_delay_versus_session_expiry() {
    let broker = broker_or_skip!();
    let will_qos = max_qos(&probe(&broker)).min(2);
    let t = topics();
    for (delay, expiry) in [(3, 5), (5, 0), (5, 2)] {
        if !will_delay_case(&broker, &t.t0, will_qos, delay, expiry) {
            return;
        }
    }
}

/// Collects from two clients until `done(a, b)` holds (counts so far) or
/// `timeout` passes, then for the quiet period.
fn receive_both(
    ca: &mut Client,
    cb: &mut Client,
    timeout: Duration,
    done: impl Fn(usize, usize) -> bool,
) -> (Vec<Publish>, Vec<Publish>) {
    let (mut at_a, mut at_b) = (Vec::new(), Vec::new());
    let deadline = Instant::now() + timeout;
    let slice = Duration::from_millis(50);
    while !done(at_a.len(), at_b.len()) && Instant::now() < deadline {
        at_a.extend(ca.collect_publishes(slice).unwrap());
        at_b.extend(cb.collect_publishes(slice).unwrap());
    }
    at_a.extend(ca.collect_publishes(quiet_period()).unwrap());
    at_b.extend(cb.collect_publishes(quiet_period()).unwrap());
    (at_a, at_b)
}

/// A clean client subscribed (in one SUBSCRIBE) to `shared` and `plain`,
/// both at QoS 2. `None` (after recording a skip) when the broker refuses
/// shared subscriptions with 0x9E.
fn shared_member(broker: &Broker, shared: &str, plain: &str) -> Option<Client> {
    let (mut c, ack) = open(broker, &connect(&unique_id("m5")));
    assert!(!ack.session_present(), "{ack:?}");
    let sub = c
        .subscribe_many(&[(shared, 2), (plain, 2)], Vec::new())
        .unwrap();
    assert_eq!(sub.codes.len(), 2, "{sub:?}");
    if sub.codes[0] == 0x9E {
        skipped("shared subscriptions not supported (SUBACK 0x9E)");
        return None;
    }
    assert!(sub.codes.iter().all(|code| *code < 0x80), "{sub:?}");
    Some(c)
}

/// A message on a shared subscription goes to exactly one member of the
/// group, while an ordinary subscription on another topic still reaches
/// both clients.
pub fn shared_subscription_single_delivery() {
    let broker = broker_or_skip!();
    let caps = probe(&broker);
    if connack_u32(&caps, prop::SHARED_SUBSCRIPTION_AVAILABLE, 1) == 0 {
        skipped("broker announces Shared Subscription Available = 0");
        return;
    }
    let t = topics();
    let shared = format!("$share/{}/{}", unique_id("g"), t.ts);
    let Some(mut ca) = shared_member(&broker, &shared, &t.t0) else {
        return;
    };
    let Some(mut cb) = shared_member(&broker, &shared, &t.t0) else {
        return;
    };
    publish_with(&mut cb, &t.t0, b"everyone", 0, false, &[]);
    let (at_a, at_b) = receive_both(&mut ca, &mut cb, default_timeout(), |a, b| a >= 1 && b >= 1);
    assert_eq!(
        (at_a.len(), at_b.len()),
        (1, 1),
        "ordinary subscription: {at_a:?} {at_b:?}"
    );
    publish_with(&mut cb, &t.ts, b"one of you", 0, false, &[]);
    let (at_a, at_b) = receive_both(&mut ca, &mut cb, default_timeout(), |a, b| a + b >= 1);
    // req: 5.0/MQTT-4.5.0-1
    assert_eq!(
        at_a.len() + at_b.len(),
        1,
        "shared subscription: {at_a:?} {at_b:?}"
    );
    bye(ca);
    bye(cb);
}
