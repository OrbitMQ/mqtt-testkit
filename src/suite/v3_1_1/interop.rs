// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Interoperability scenarios: end-to-end client workflows (publish/subscribe at every QoS,
//! retained messages, wills, persistent sessions, keep-alive, ...) that exercise a broker the way
//! real applications do. Written from behavioural descriptions only; no third-party code.
//!
//! Every scenario is self-contained: it uses its own client identifiers and its own topic tree
//! under a unique prefix `P`, so it is safe on a shared broker and in parallel with other cases.
//! The tree, relative to `P`:
//!
//! | Name | Topic | | Filter | Shape |
//! |---|---|---|---|---|
//! | T0 | `P/N1` | | W-N1-plus | `P/N1/+` |
//! | T1 | `P/N1/x` | | W-plus-plus | `P/+/+` (also matches T4) |
//! | T2 | `P/N2/c` | | W-N1-hash | `P/N1/#` |
//! | T3 | `P/N1/c` | | | |
//! | T4 | `P//N1` | | | |
//!
//! A client "delivers" a message the way an application client would: QoS 0 and 1 on PUBLISH,
//! QoS 2 on PUBREL (see [`Receiver`]).

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::{
    Ack, Broker, Client, Connect, Event, Packet, Publish, Will, broker_or_skip, default_timeout,
    quiet_period, unique_id,
};

use super::V;

/// Environment variable naming a topic filter the broker under test refuses to
/// let these clients subscribe to (`subscribe_refused_by_policy`).
const DENY_FILTER_ENV: &str = "MQTT_TESTKIT_INTEROP_DENY_FILTER";

/// The scenario topic tree under a prefix unique to one case.
struct Tree {
    prefix: String,
}

impl Tree {
    fn new() -> Self {
        Self {
            prefix: format!("interop/{}", unique_id("i311")),
        }
    }
    fn t0(&self) -> String {
        format!("{}/N1", self.prefix)
    }
    fn t1(&self) -> String {
        format!("{}/N1/x", self.prefix)
    }
    fn t2(&self) -> String {
        format!("{}/N2/c", self.prefix)
    }
    fn t3(&self) -> String {
        format!("{}/N1/c", self.prefix)
    }
    /// The prefix followed by an empty level, so `+` matches it.
    fn t4(&self) -> String {
        format!("{}//N1", self.prefix)
    }
    fn n1_plus(&self) -> String {
        format!("{}/N1/+", self.prefix)
    }
    fn plus_plus(&self) -> String {
        format!("{}/+/+", self.prefix)
    }
    fn n1_hash(&self) -> String {
        format!("{}/N1/#", self.prefix)
    }
}

/// A CONNECT with the scenario defaults: Keep Alive 0, the given Clean Session.
fn connect_packet(client_id: &str, clean: bool) -> Connect {
    Connect::new(V, client_id).clean(clean).keep_alive(0)
}

/// Connects and requires return code 0; returns the client and Session Present.
fn connect(broker: &Broker, client_id: &str, clean: bool) -> (Client, bool) {
    let (c, ack) = broker
        .connect_ok(&connect_packet(client_id, clean))
        .unwrap_or_else(|e| panic!("connect {client_id} (clean={clean}): {e}"));
    (c, ack.session_present())
}

/// Sends DISCONNECT, closes the client's side (the client MUST close,
/// MQTT-3.14.4-1; the server only SHOULD, §3.14.4) and waits briefly for the
/// broker to finish with the connection before the scenario goes on. A
/// delivery that raced the DISCONNECT is irrelevant here.
fn disconnect(c: Client) {
    c.disconnect_and_drain()
        .unwrap_or_else(|e| panic!("DISCONNECT: {e}"));
}

/// Erases any session the broker holds for `client_id` (Clean Session = 1 connect, then
/// DISCONNECT).
fn forget(broker: &Broker, client_id: &str) {
    let (c, _) = connect(broker, client_id, true);
    disconnect(c);
}

/// Application-level receiver for one client: acknowledges every incoming
/// PUBLISH immediately and records a delivery on PUBLISH (QoS 0/1) or on PUBREL
/// (QoS 2). A re-sent QoS 2 PUBLISH whose PUBREL has not arrived yet is not
/// counted twice.
struct Receiver {
    pending: Vec<Publish>,
}

impl Receiver {
    const fn new() -> Self {
        Self {
            pending: Vec::new(),
        }
    }

    /// Delivers messages until at least `want` arrived (waiting up to
    /// [`default_timeout`] for each), then keeps listening until nothing has
    /// happened for [`quiet_period`] and no QoS 2 flow is open. Returns every
    /// delivery in order. A close or any packet other than PUBLISH/PUBREL fails.
    fn receive(&mut self, c: &mut Client, want: usize) -> Vec<Publish> {
        let mut out = Vec::new();
        let patience = |delivered: usize, open_flows: bool| {
            if delivered < want || open_flows {
                default_timeout()
            } else {
                quiet_period()
            }
        };
        let mut deadline = Instant::now() + patience(out.len(), !self.pending.is_empty());
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                assert!(
                    self.pending.is_empty(),
                    "no PUBREL for QoS 2 deliveries {:?}",
                    self.pending
                );
                return out;
            }
            match c.next_event(left).unwrap() {
                Event::Timeout => continue,
                Event::Closed => panic!(
                    "broker closed the connection while receiving (delivered so far: {out:?})"
                ),
                Event::Packet(Packet::Publish(p)) => match (p.qos, p.packet_id) {
                    (0, _) => out.push(p),
                    (1, Some(id)) => {
                        c.send(&Packet::PubAck(Ack::new(id))).unwrap();
                        out.push(p);
                    }
                    (2, Some(id)) => {
                        c.send(&Packet::PubRec(Ack::new(id))).unwrap();
                        if !self.pending.iter().any(|q| q.packet_id == Some(id)) {
                            self.pending.push(p);
                        }
                    }
                    _ => panic!("invalid PUBLISH from the broker: {p:?}"),
                },
                Event::Packet(Packet::PubRel(a)) => {
                    c.send(&Packet::PubComp(Ack::new(a.packet_id))).unwrap();
                    if let Some(i) = self
                        .pending
                        .iter()
                        .position(|q| q.packet_id == Some(a.packet_id))
                    {
                        out.push(self.pending.remove(i));
                    }
                }
                Event::Packet(other) => panic!("unexpected packet while receiving: {other:?}"),
            }
            deadline = Instant::now() + patience(out.len(), !self.pending.is_empty());
        }
    }
}

/// Receives with a fresh [`Receiver`] (see [`Receiver::receive`]).
fn receive(c: &mut Client, want: usize) -> Vec<Publish> {
    Receiver::new().receive(c, want)
}

fn topics(got: &[Publish]) -> Vec<&str> {
    got.iter().map(|p| p.topic.as_str()).collect()
}

/// Basic connect/disconnect and self-delivery at QoS 0, 1 and 2 on one topic; then a second
/// CONNECT on a connected network connection must be treated as a protocol violation (close, no
/// CONNACK), and a CONNECT with an unknown protocol name must not be accepted.
pub fn connect_publish_subscribe_roundtrip() {
    let broker = broker_or_skip!();
    let tree = Tree::new();
    let id = unique_id("ia");

    // Step 1: connect and disconnect.
    let (c, _) = connect(&broker, &id, true);
    disconnect(c);

    // Step 2: connect again; a clean session never reports a stored one.
    let (mut c, present) = connect(&broker, &id, true);
    // req: 3.1.1/MQTT-3.2.2-1
    assert!(!present, "Session Present = 1 for Clean Session = 1");

    // Steps 3-5: subscribe to T0 and publish to it at every QoS.
    let t0 = tree.t0();
    let granted = c.subscribe(&t0, 2).unwrap().codes[0];
    assert!(granted <= 2, "subscription to {t0} refused: {granted:#04x}");
    for qos in 0..=2u8 {
        c.publish(&t0, format!("qos{qos}").as_bytes(), qos, false)
            .unwrap();
    }
    let got = receive(&mut c, 3);
    assert_eq!(got.len(), 3, "expected 3 deliveries on {t0}, got {got:?}");
    for qos in 0..=2u8 {
        let payload = format!("qos{qos}");
        let p = got
            .iter()
            .find(|p| p.payload == payload.as_bytes())
            .unwrap_or_else(|| panic!("{payload:?} not delivered: {got:?}"));
        assert_eq!(p.topic, t0);
        // req: 3.1.1/MQTT-3.8.4-6
        assert_eq!(p.qos, qos.min(granted), "delivered QoS of {payload}: {p:?}");
    }
    disconnect(c);

    // Step 6: a second CONNECT on an established connection.
    let (mut c, _) = connect(&broker, &id, true);
    c.send(&Packet::Connect(Box::new(connect_packet(&id, true))))
        .unwrap();
    let before_close = c.expect_closed(default_timeout()).unwrap();
    // req: 3.1.1/MQTT-3.1.0-2
    assert!(
        before_close
            .iter()
            .all(|p| !matches!(p, Packet::ConnAck(_))),
        "second CONNECT answered with {before_close:?}"
    );

    // Step 7: a CONNECT whose Protocol Name is neither MQTT nor MQIsdp.
    let mut c = broker.open(V).unwrap();
    c.send(&Packet::Connect(Box::new(
        connect_packet(&id, true).protocol("Bogus", 4),
    )))
    .unwrap();
    // req: 3.1.1/MQTT-3.1.2-1
    let refused = c
        .expect_refused(default_timeout())
        .unwrap_or_else(|e| panic!("CONNECT with protocol name \"Bogus\" not refused: {e}"));
    crate::outcome::note(match refused {
        Some(ack) => format!(
            "unknown protocol name: CONNACK {:#04x}, then close",
            ack.code
        ),
        None => "unknown protocol name: closed without CONNACK".to_owned(),
    });
}

/// Retained messages published at QoS 0, 1 and 2 are delivered (RETAIN = 1, original QoS) to a
/// later `+/+` subscriber; zero-length retained publishes then remove all three and are not stored
/// themselves.
pub fn retained_messages_set_and_clear() {
    let broker = broker_or_skip!();
    let tree = Tree::new();
    let id = unique_id("ia");
    let set = [(tree.t1(), 0u8), (tree.t2(), 1), (tree.t3(), 2)];

    let (mut c, present) = connect(&broker, &id, true);
    assert!(!present);
    // The QoS 0 publish has no acknowledgement; it precedes the QoS 1/2
    // flows on the same connection, so their completion orders it too.
    for (t, qos) in &set {
        c.publish(t, format!("retained{qos}").as_bytes(), *qos, true)
            .unwrap();
    }
    let granted = c.subscribe(&tree.plus_plus(), 2).unwrap().codes[0];
    assert!(
        granted <= 2,
        "wildcard subscription refused: {granted:#04x}"
    );
    let got = receive(&mut c, 3);
    for (t, qos) in &set {
        let Some(p) = got.iter().find(|p| &p.topic == t) else {
            // A server MAY discard a retained QoS 0 message at any time (MQTT-3.3.1-7).
            assert_eq!(*qos, 0, "no retained message for {t}: {got:?}");
            crate::outcome::note(format!("retained QoS 0 message on {t} was not kept"));
            continue;
        };
        // req: 3.1.1/MQTT-3.3.1-5
        // req: 3.1.1/MQTT-3.3.1-6
        assert_eq!(p.payload, format!("retained{qos}").as_bytes());
        // req: 3.1.1/MQTT-3.3.1-8
        assert!(p.retain, "retained delivery without RETAIN: {p:?}");
        // req: 3.1.1/MQTT-3.8.4-6
        assert_eq!(p.qos, (*qos).min(granted), "{p:?}");
    }
    disconnect(c);

    // Clear them with zero-length retained publishes on a fresh clean session.
    let (mut c, present) = connect(&broker, &id, true);
    assert!(!present);
    for (t, qos) in &set {
        c.publish(t, b"", *qos, true).unwrap();
    }
    c.subscribe(&tree.plus_plus(), 2).unwrap();
    let got = receive(&mut c, 0);
    // req: 3.1.1/MQTT-3.3.1-10
    // req: 3.1.1/MQTT-3.3.1-11
    assert!(got.is_empty(), "retained messages not cleared: {got:?}");
    disconnect(c);
}

/// A zero-length Client Identifier must be refused with Clean Session = 0, and may be accepted
/// with Clean Session = 1 (if the broker refuses it there too, it must use return code 0x02).
pub fn empty_client_id_requires_clean_session() {
    let broker = broker_or_skip!();

    let mut c = broker.open(V).unwrap();
    c.send(&Packet::Connect(Box::new(connect_packet("", false))))
        .unwrap();
    let refused = c
        .expect_refused(default_timeout())
        .unwrap_or_else(|e| panic!("zero-length client id with Clean Session = 0: {e}"));
    match refused {
        Some(ack) if ack.code == 0x02 => {
            // req: 3.1.1/MQTT-3.1.3-8
            assert_eq!(ack.code, 0x02);
        }
        Some(ack) => crate::outcome::note(format!(
            "zero-length client id with Clean Session = 0 refused with {:#04x} (0x02 expected)",
            ack.code
        )),
        None => crate::outcome::note(
            "zero-length client id with Clean Session = 0: closed without CONNACK".to_owned(),
        ),
    }

    let (c, ack) = broker.connect(&connect_packet("", true)).unwrap();
    if ack.code == 0 {
        // req: 3.1.1/MQTT-3.1.3-6
        assert!(!ack.session_present());
        disconnect(c);
    } else {
        // The broker does not allow zero-length ids at all: then only 0x02.
        // req: 3.1.1/MQTT-3.1.3-9
        assert_eq!(ack.code, 0x02, "zero-length client id refused with {ack:?}");
        crate::outcome::note("broker refuses zero-length client ids even with Clean Session = 1");
    }
}

/// QoS 1 and QoS 2 messages matching a persistent session's subscription are queued while the
/// client is offline and delivered when it reconnects; queueing QoS 0 is optional.
pub fn offline_queue_for_persistent_session() {
    let broker = broker_or_skip!();
    let tree = Tree::new();
    let id_a = unique_id("ia");
    let id_b = unique_id("ib");

    forget(&broker, &id_a);
    let (mut a, _) = connect(&broker, &id_a, false);
    let granted = a.subscribe(&tree.plus_plus(), 2).unwrap().codes[0];
    assert!(
        granted <= 2,
        "wildcard subscription refused: {granted:#04x}"
    );
    disconnect(a);

    let (mut b, present) = connect(&broker, &id_b, true);
    assert!(!present, "Session Present = 1 for Clean Session = 1");
    let sent = [(tree.t1(), 0u8), (tree.t2(), 1), (tree.t3(), 2)];
    for (t, qos) in &sent {
        b.publish(t, format!("offline{qos}").as_bytes(), *qos, false)
            .unwrap();
    }
    disconnect(b);

    let (mut a, present) = connect(&broker, &id_a, false);
    // req: 3.1.1/MQTT-3.2.2-2
    assert!(present, "stored session not reported");
    let got = receive(&mut a, 2);
    disconnect(a);
    forget(&broker, &id_a);

    assert!(
        got.len() == 2 || got.len() == 3,
        "expected 2 or 3 queued messages, got {got:?}"
    );
    for (t, qos) in &sent {
        let found = got.iter().find(|p| &p.topic == t);
        if *qos == 0 {
            crate::outcome::note(if found.is_some() {
                "QoS 0 messages are queued for offline sessions"
            } else {
                "QoS 0 messages are not queued for offline sessions"
            });
            continue;
        }
        let p = found.unwrap_or_else(|| panic!("QoS {qos} message on {t} not queued: {got:?}"));
        // req: 3.1.1/MQTT-3.1.2-5
        // req: 3.1.1/MQTT-4.5.0-1
        assert_eq!(p.payload, format!("offline{qos}").as_bytes());
        assert_eq!(p.qos, (*qos).min(granted), "{p:?}");
    }
    assert_eq!(
        got.iter().map(|p| &p.topic).collect::<BTreeSet<_>>().len(),
        got.len(),
        "a queued message was delivered twice: {got:?}"
    );
}

/// Two subscriptions of one client (`N1/#` at QoS 2 and `N1/+` at QoS 1, in one SUBSCRIBE) both
/// match a QoS 2 message: the broker delivers one copy at QoS 2, or one copy per subscription
/// (QoS 2 and QoS 1).
pub fn overlapping_filters_single_or_per_subscription() {
    let broker = broker_or_skip!();
    let tree = Tree::new();
    let (mut c, _) = connect(&broker, &unique_id("ia"), true);
    let (hash, plus) = (tree.n1_hash(), tree.n1_plus());
    let ack = c
        .subscribe_many(&[(hash.as_str(), 2), (plus.as_str(), 1)], Vec::new())
        .unwrap();
    let [g_hash, g_plus] = ack.codes[..] else {
        panic!("SUBACK for two filters: {ack:?}");
    };
    assert!(g_hash <= 2 && g_plus <= 1, "{ack:?}");
    c.publish(&tree.t3(), b"overlap", 2, false).unwrap();
    let got = receive(&mut c, 1);
    disconnect(c);

    assert!(
        got.iter()
            .all(|p| p.topic == tree.t3() && p.payload == b"overlap"),
        "{got:?}"
    );
    let mut qos: Vec<u8> = got.iter().map(|p| p.qos).collect();
    qos.sort_unstable();
    match qos.len() {
        1 => {
            // req: 3.1.1/MQTT-3.3.5-1
            assert_eq!(qos[0], g_hash.max(g_plus), "single copy: {got:?}");
            crate::outcome::note("overlapping subscriptions: one copy at the highest QoS");
        }
        2 => {
            let mut want = vec![g_hash, g_plus];
            want.sort_unstable();
            // req: 3.1.1/MQTT-3.3.5-1
            assert_eq!(qos, want, "one copy per subscription: {got:?}");
            crate::outcome::note("overlapping subscriptions: one copy per subscription");
        }
        _ => panic!("expected one or two copies, got {got:?}"),
    }
}

/// A client with Keep Alive 5 s that then sends nothing is disconnected within 1.5 x Keep Alive,
/// and its Will (QoS 2, on a topic with an empty first level) reaches a subscriber exactly once.
pub fn keep_alive_expiry_publishes_will() {
    let broker = broker_or_skip!();
    let tree = Tree::new();
    let t4 = tree.t4();
    let keep_alive = Duration::from_secs(5);
    let mut will = Will::new(&t4, b"keep-alive will");
    will.qos = 2;

    let (mut a, ack) = broker
        .connect_ok(
            &connect_packet(&unique_id("ia"), true)
                .keep_alive(5)
                .will(will),
        )
        .unwrap();
    let silent_since = Instant::now();
    assert_eq!(ack.code, 0);
    let (mut b, _) = connect(&broker, &unique_id("ib"), true);
    let granted = b.subscribe(&t4, 2).unwrap().codes[0];
    assert!(granted <= 2, "subscription to {t4} refused: {granted:#04x}");

    // Allow generous slack for slow brokers; never fail before 10 s.
    let limit = (keep_alive.mul_f32(1.5) + default_timeout()).max(Duration::from_secs(10));
    let left = limit.saturating_sub(silent_since.elapsed());
    let before_close = a
        .expect_closed(left)
        .unwrap_or_else(|e| panic!("silent client not disconnected within {limit:?}: {e}"));
    let after = silent_since.elapsed();
    // req: 3.1.1/MQTT-3.1.2-24
    assert!(
        before_close.is_empty(),
        "keep-alive close preceded by {before_close:?}"
    );
    crate::outcome::note(format!("keep-alive (5 s) close after {after:.1?}"));

    let got = receive(&mut b, 1);
    disconnect(b);
    // req: 3.1.1/MQTT-3.1.2-8
    assert_eq!(got.len(), 1, "expected exactly the will, got {got:?}");
    assert_eq!(
        (got[0].topic.as_str(), got[0].payload.as_slice()),
        (t4.as_str(), &b"keep-alive will"[..])
    );
    // req: 3.1.1/MQTT-3.8.4-6
    assert_eq!(got[0].qos, granted.min(2), "{:?}", got[0]);
}

/// A persistent-session client that leaves without acknowledging the QoS 1 and QoS 2 PUBLISH
/// packets it received gets both again (DUP = 1) after reconnecting with Clean Session = 0.
pub fn resend_unacknowledged_on_session_resume() {
    let broker = broker_or_skip!();
    let tree = Tree::new();
    let id = unique_id("ib");

    forget(&broker, &id);
    let (mut b, _) = connect(&broker, &id, false);
    let granted = b.subscribe(&tree.n1_hash(), 2).unwrap().codes[0];
    if granted == 0 {
        crate::outcome::skipped("broker granted QoS 0 for a QoS 2 subscription");
        disconnect(b);
        forget(&broker, &id);
        return;
    }
    assert!(granted <= 2, "subscription refused: {granted:#04x}");

    // Non-acknowledging mode: complete the outgoing flows (PUBACK; PUBREC ->
    // PUBREL -> PUBCOMP) but leave incoming PUBLISH packets unanswered.
    let (t1, t3) = (tree.t1(), tree.t3());
    b.publish(&t1, b"", 1, false).unwrap();
    b.publish(&t3, b"", 2, false).unwrap();
    // A server may keep only one of them in flight, the other queued.
    let ignored = b
        .expect_in_flight(2, default_timeout())
        .unwrap_or_else(|e| panic!("own publishes not forwarded before going offline: {e}"));
    disconnect(b);

    let (mut b, present) = connect(&broker, &id, false);
    // req: 3.1.1/MQTT-3.2.2-2
    assert!(present, "stored session not reported");
    let got = receive(&mut b, 2);
    disconnect(b);
    forget(&broker, &id);

    assert_eq!(
        got.len(),
        2,
        "expected the 2 unacknowledged messages, got {got:?}"
    );
    for (t, qos) in [(&t1, 1u8), (&t3, 2)] {
        let Some(first) = ignored.iter().find(|p| &p.topic == t) else {
            // Never sent before going offline: delivered now as a new message.
            assert!(
                got.iter().any(|p| &p.topic == t),
                "{t} lost: {:?}",
                topics(&got)
            );
            continue;
        };
        let again = got
            .iter()
            .find(|p| &p.topic == t)
            .unwrap_or_else(|| panic!("{t} not re-sent: {:?}", topics(&got)));
        assert_eq!(again.qos, qos.min(granted), "{again:?}");
        // req: 3.1.1/MQTT-4.4.0-1
        assert_eq!(again.packet_id, first.packet_id, "{first:?} vs {again:?}");
        // req: 3.1.1/MQTT-3.3.1-1
        assert!(again.dup, "re-sent PUBLISH without DUP: {again:?}");
    }
}

/// The broker refuses a subscription to a filter its policy denies with SUBACK return code 0x80.
/// The filter comes from `MQTT_TESTKIT_INTEROP_DENY_FILTER`; without it a fresh filter under a unique
/// prefix is tried, and the scenario skips if the broker grants it.
pub fn subscribe_refused_by_policy() {
    let broker = broker_or_skip!();
    let filter = crate::env_var(DENY_FILTER_ENV)
        .unwrap_or_else(|| format!("{}/nosubscribe", Tree::new().prefix));
    let (mut c, _) = connect(&broker, &unique_id("ia"), true);
    let ack = c
        .subscribe(&filter, 2)
        .unwrap_or_else(|e| panic!("SUBSCRIBE to denied filter {filter:?}: {e}"));
    disconnect(c);
    // req: 3.1.1/MQTT-3.9.3-2
    assert_eq!(ack.codes.len(), 1, "{ack:?}");
    match ack.codes[0] {
        0x80 => {}
        code @ 0..=2 => crate::outcome::skipped(format!(
            "broker granted QoS {code} for {filter:?}; configure a policy denying a filter and \
             name it in {DENY_FILTER_ENV}"
        )),
        code => panic!("invalid SUBACK return code {code:#04x}"),
    }
}

/// A topic starting with `$` must not match a filter starting with a wildcard: a client subscribed
/// to `+/<leaf>` publishes to `$<n1>/<leaf>` and receives nothing. Refusing the `$` publish is also
/// accepted.
pub fn dollar_topics_hidden_from_leading_wildcards() {
    let broker = broker_or_skip!();
    // `$` topics live at the root, so the prefix becomes part of the first level and
    // the leaf level carries the uniqueness instead.
    let n1 = unique_id("N1");
    let leaf = unique_id("x");
    let (dollar, plain) = (format!("${n1}/{leaf}"), format!("{n1}/{leaf}"));
    let filter = format!("+/{leaf}");

    let (mut b, _) = connect(&broker, &unique_id("ib"), true);
    b.subscribe(&filter, 2).unwrap();
    // Discard anything already there (e.g. retained messages).
    let _ = receive(&mut b, 0);
    let (mut exact, _) = connect(&broker, &unique_id("ic"), true);
    exact.subscribe(&dollar, 2).unwrap();

    let mut p = Publish::new(&dollar, b"");
    p.qos = 1;
    if let Err(e) = b.publish_packet(p) {
        crate::outcome::note(format!("broker refused the PUBLISH to {dollar}: {e}"));
        disconnect(exact);
        return;
    }
    let got = receive(&mut b, 0);
    // req: 3.1.1/MQTT-4.7.2-1
    assert!(got.is_empty(), "{filter} matched {:?}", topics(&got));
    let routed = receive(&mut exact, 0);
    if routed.is_empty() {
        crate::outcome::note(format!(
            "broker accepted but did not route the PUBLISH to {dollar} (no exact subscriber got it)"
        ));
    }
    disconnect(exact);

    // Control: the same leaf under a plain first level does match.
    b.publish(&plain, b"", 1, false).unwrap();
    let got = receive(&mut b, 1);
    disconnect(b);
    assert_eq!(
        topics(&got),
        [plain.as_str()],
        "control topic not delivered"
    );
}

/// After UNSUBSCRIBE of one of three subscriptions, messages on that topic stop while the other
/// two subscriptions keep delivering.
pub fn unsubscribe_stops_delivery() {
    let broker = broker_or_skip!();
    let tree = Tree::new();
    let (t0, t1, t2) = (tree.t0(), tree.t1(), tree.t2());

    let (mut b, _) = connect(&broker, &unique_id("ib"), true);
    for t in [&t0, &t1, &t2] {
        let code = b.subscribe(t, 2).unwrap().codes[0];
        assert!(code <= 2, "subscription to {t} refused: {code:#04x}");
    }
    // Discard anything delivered before the publishes (retained messages).
    let _ = receive(&mut b, 0);
    b.unsubscribe(&t0).unwrap();

    let (mut a, _) = connect(&broker, &unique_id("ia"), true);
    for t in [&t0, &t1, &t2] {
        a.publish(t, b"after-unsubscribe", 1, false).unwrap();
    }
    let got = receive(&mut b, 2);
    disconnect(a);
    disconnect(b);

    let mut seen = topics(&got);
    seen.sort_unstable();
    let mut want = vec![t1.as_str(), t2.as_str()];
    want.sort_unstable();
    // req: 3.1.1/MQTT-3.10.4-2
    assert_eq!(seen, want, "deliveries after unsubscribing {t0}: {got:?}");
    assert!(got.iter().all(|p| !p.retain), "{got:?}");
}

/// A client whose connection drops without DISCONNECT has its
/// Will (QoS 2) delivered exactly once to a persistent-session subscriber.
pub fn abrupt_disconnect_publishes_will() {
    let broker = broker_or_skip!();
    let tree = Tree::new();
    let t2 = tree.t2();
    let id_b = unique_id("ib");
    let mut will = Will::new(&t2, b"abrupt will");
    will.qos = 2;

    forget(&broker, &id_b);
    let (a, _) = broker
        .connect_ok(
            &connect_packet(&unique_id("ia"), true)
                .keep_alive(2)
                .will(will),
        )
        .unwrap();
    let (mut b, _) = connect(&broker, &id_b, false);
    b.subscribe(&t2, 2).unwrap();
    a.abort();

    let got = receive(&mut b, 1);
    disconnect(b);
    forget(&broker, &id_b);
    // req: 3.1.1/MQTT-3.1.2-8
    assert_eq!(got.len(), 1, "expected exactly the will, got {got:?}");
    assert_eq!(
        (got[0].topic.as_str(), got[0].payload.as_slice()),
        (t2.as_str(), &b"abrupt will"[..])
    );
}
