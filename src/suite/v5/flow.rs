// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Flow control (Receive Maximum), Maximum Packet Size and Topic Aliases
//! (MQTT 5.0).

use std::collections::HashMap;
use std::time::Duration;

use crate::{
    Ack, Connect, Packet, PropValue, Property, Publish, broker_or_skip, default_timeout, find_prop,
    prop, quiet_period, unique_id,
};

use super::{V, connack_u32, session, session_with_ack, topic};

fn alias(n: u16) -> Property {
    Property::new(prop::TOPIC_ALIAS, PropValue::TwoByte(n))
}

pub fn server_respects_the_clients_receive_maximum() {
    let broker = broker_or_skip!();
    let t = topic();
    let (mut sub, _) = broker
        .connect_ok(
            &Connect::new(V, &unique_id("rm"))
                .property(prop::RECEIVE_MAXIMUM, PropValue::TwoByte(2)),
        )
        .unwrap();
    sub.subscribe_needing(&t, 1);
    let mut publisher = session(&broker);
    for i in 0..5u8 {
        publisher.publish(&t, &[i], 1, false).unwrap();
    }
    // The initial send quota is not zero; a server may use less than all of
    // it (down to one message at a time) ...
    // req: 5.0/MQTT-4.9.0-1
    let mut window = sub.expect_in_flight(2, default_timeout()).unwrap();
    // ... and not above the client's Receive Maximum.
    // req: 5.0/MQTT-3.3.4-9
    // req: 5.0/MQTT-4.9.0-2
    // req: 5.0/MQTT-4.9.0-1
    sub.expect_no_packet(quiet_period()).unwrap();
    // Non-PUBLISH traffic is not held back by the full window.
    // req: 5.0/MQTT-3.3.4-10
    // req: 5.0/MQTT-4.9.0-3
    sub.ping().unwrap();
    let mut order = Vec::new();
    let mut checked_refill = false;
    while let Some(oldest) = (!window.is_empty()).then(|| window.remove(0)) {
        sub.send(&Packet::PubAck(Ack::new(oldest.packet_id.unwrap())))
            .unwrap();
        order.push(oldest.payload[0]);
        // Each PUBACK frees one slot.
        if order.len() + window.len() < 5 {
            window.push(sub.expect_publish_unacked(default_timeout()).unwrap());
            if window.len() == 2 && order.len() + window.len() < 5 && !checked_refill {
                // req: 5.0/MQTT-3.3.4-9
                sub.expect_no_packet(quiet_period()).unwrap();
                checked_refill = true;
            }
        }
    }
    // req: 5.0/MQTT-4.6.0-5
    assert_eq!(order, [0, 1, 2, 3, 4]);
}

pub fn receive_maximum_counts_qos2_until_pubcomp() {
    let broker = broker_or_skip!();
    let t = topic();
    let (mut sub, _) = broker
        .connect_ok(
            &Connect::new(V, &unique_id("rm2"))
                .property(prop::RECEIVE_MAXIMUM, PropValue::TwoByte(1)),
        )
        .unwrap();
    sub.subscribe_needing(&t, 2);
    let mut publisher = session(&broker);
    publisher.publish(&t, b"a", 2, false).unwrap();
    publisher.publish(&t, b"b", 2, false).unwrap();
    let p = sub.expect_publish_unacked(default_timeout()).unwrap();
    let id = p.packet_id.unwrap();
    sub.send(&Packet::PubRec(Ack::new(id))).unwrap();
    match sub.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(a) => assert_eq!(a.packet_id, id),
        other => panic!("expected PUBREL, got {other:?}"),
    }
    // Still in flight until PUBCOMP: no second PUBLISH yet.
    // req: 5.0/MQTT-3.3.4-9
    sub.expect_no_packet(quiet_period()).unwrap();
    sub.send(&Packet::PubComp(Ack::new(id))).unwrap();
    // No deadline for the next delivery (a server may release it from a
    // timer), but it is a first transmission.
    // req: 5.0/MQTT-3.3.4-9
    let b = sub.expect_publish(Duration::from_secs(15)).unwrap();
    assert_eq!(b.payload, b"b");
    // req: 5.0/MQTT-3.3.1-3
    assert!(!b.dup, "first transmission with DUP=1: {b:?}");
}

/// Strict: MQTT-3.3.4-7 binds the client; rejecting relies on the server detecting it.
pub fn exceeding_the_servers_receive_maximum_is_an_error_0x93() {
    let broker = broker_or_skip!();
    let (mut c, ack) = session_with_ack(&broker);
    let max = connack_u32(&ack, prop::RECEIVE_MAXIMUM, 65_535);
    // With 65535 every packet identifier would be in flight, so the window
    // cannot be exceeded with a new message.
    let Ok(extra) = u16::try_from(max + 1) else {
        crate::outcome::skipped("Receive Maximum 65535 leaves no free packet identifier");
        return;
    };
    let t = topic();
    // QoS 2 PUBLISHes stay unacknowledged (in flight) until PUBREL, which is
    // never sent. PUBRECs are drained in batches so neither side blocks.
    let mut next = 0u32;
    let batch = 500;
    while next < max {
        let end = (next + batch).min(max);
        for i in next..end {
            let mut p = Publish::new(&t, b"");
            p.qos = 2;
            p.packet_id = Some(u16::try_from(i + 1).unwrap());
            c.send(&Packet::Publish(p)).unwrap();
        }
        for _ in next..end {
            match c.expect_packet(default_timeout()).unwrap() {
                Packet::PubRec(_) => {}
                other => panic!("expected PUBREC, got {other:?}"),
            }
        }
        next = end;
    }
    // One more: Receive Maximum exceeded.
    let mut p = Publish::new(&t, b"");
    p.qos = 2;
    p.packet_id = Some(extra);
    c.send(&Packet::Publish(p)).unwrap();
    // Either the connection ends (DISCONNECT 0x93 if any), or the excess
    // PUBLISH is refused with an error PUBREC (e.g. 0x97 Quota exceeded).
    match c.next_event(default_timeout()).unwrap() {
        crate::Event::Packet(Packet::PubRec(a)) => {
            // req: 5.0/MQTT-3.3.4-7
            assert!(
                a.packet_id == extra && a.reason.unwrap_or(0) >= 0x80,
                "excess PUBLISH accepted: {a:?}"
            );
        }
        crate::Event::Packet(Packet::Disconnect(d)) => {
            // req: 5.0/MQTT-3.3.4-7
            assert_eq!(d.reason.unwrap_or(0), 0x93, "Receive Maximum exceeded");
            c.expect_closed(default_timeout()).unwrap();
        }
        crate::Event::Closed => {}
        other => panic!("excess PUBLISH: expected a refusal, got {other:?}"),
    }
}

pub fn server_never_sends_packets_above_the_clients_maximum_packet_size() {
    let broker = broker_or_skip!();
    let t = topic();
    let limit = 128u32;
    let (mut sub, _) = broker
        .connect_ok(
            &Connect::new(V, &unique_id("mp"))
                .property(prop::MAXIMUM_PACKET_SIZE, PropValue::FourByte(limit)),
        )
        .unwrap();
    sub.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, &[b'x'; 512], 1, false).unwrap();
    publisher.publish(&t, b"small", 1, false).unwrap();
    let got = sub.collect_publishes(quiet_period()).unwrap();
    for p in &got {
        let size = Packet::Publish(p.clone()).encode(V).len();
        // req: 5.0/MQTT-3.1.2-24
        assert!(size <= limit as usize, "{size}-byte PUBLISH sent: {p:?}");
    }
    // The oversized message is discarded; delivery continues.
    // req: 5.0/MQTT-3.1.2-25
    assert_eq!(
        got.iter().map(|p| p.payload.clone()).collect::<Vec<_>>(),
        [b"small".to_vec()]
    );
}

pub fn oversized_message_does_not_block_the_receive_window() {
    let broker = broker_or_skip!();
    let t = topic();
    let (mut sub, _) = broker
        .connect_ok(
            &Connect::new(V, &unique_id("mpw"))
                .property(prop::MAXIMUM_PACKET_SIZE, PropValue::FourByte(64))
                .property(prop::RECEIVE_MAXIMUM, PropValue::TwoByte(1)),
        )
        .unwrap();
    sub.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    for _ in 0..3 {
        publisher.publish(&t, &[b'x'; 200], 1, false).unwrap();
    }
    publisher.publish(&t, b"ok", 1, false).unwrap();
    // Discarded messages count as delivered, so they free their slot.
    // req: 5.0/MQTT-3.1.2-25
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().payload,
        b"ok"
    );
}

/// Strict: MQTT-3.2.2-15 binds the client; closing relies on detection.
pub fn packet_above_the_servers_maximum_packet_size_is_an_error_0x95() {
    let broker = broker_or_skip!();
    let (mut c, ack) = session_with_ack(&broker);
    let Some(PropValue::FourByte(max)) = find_prop(&ack.properties, prop::MAXIMUM_PACKET_SIZE)
    else {
        crate::outcome::skipped("broker announces no Maximum Packet Size");
        return;
    };
    let max = *max as usize;
    if max > 16 * 1024 * 1024 {
        crate::outcome::skipped(format!("Maximum Packet Size {max} too large to test"));
        return;
    }
    // The smallest QoS 0 PUBLISH larger than max. Some sizes cannot be
    // encoded (the Remaining Length grows by a byte at 128, 16384, ...), so
    // this is max + 1 or a byte more.
    let t = topic();
    let empty = Packet::Publish(Publish::new(&t, b"")).encode(V).len();
    let mut payload_len = (max + 1).saturating_sub(empty);
    while Packet::Publish(Publish::new(&t, &vec![0; payload_len]))
        .encode(V)
        .len()
        <= max
    {
        payload_len += 1;
    }
    let bytes = Packet::Publish(Publish::new(&t, &vec![0; payload_len])).encode(V);
    assert!(bytes.len() > max);
    let _ = c.send_raw(&bytes);
    // req: 5.0/MQTT-3.2.2-15
    let reason = c.expect_error_close(default_timeout()).unwrap();
    if let Some(r) = reason {
        // req: 5.0/MQTT-3.2.2-15
        assert_eq!(r, 0x95, "Packet too large");
    }
}

/// Server's Topic Alias Maximum, or `None` (after printing a skip) if 0.
fn server_alias_max(ack: &crate::ConnAck) -> Option<u16> {
    let max = u16::try_from(connack_u32(ack, prop::TOPIC_ALIAS_MAXIMUM, 0)).unwrap();
    if max == 0 {
        crate::outcome::skipped("Topic Alias Maximum is 0");
        None
    } else {
        Some(max)
    }
}

pub fn client_topic_alias_is_accepted_and_resolved() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (mut c, ack) = session_with_ack(&broker);
    let Some(max) = server_alias_max(&ack) else {
        return;
    };
    for n in [1, max] {
        let mut first = Publish::new(&t, b"set");
        first.properties = vec![alias(n)];
        c.publish_packet(first).unwrap();
        let mut by_alias = Publish::new("", b"use");
        by_alias.properties = vec![alias(n)];
        c.publish_packet(by_alias).unwrap();
        let got = sub.expect_publishes(2, default_timeout()).unwrap();
        for p in &got {
            // req: 5.0/MQTT-3.3.2-12
            assert_eq!(p.topic, t, "alias {n}: {p:?}");
        }
        assert_eq!(got[1].payload, b"use");
    }
    c.ping().unwrap();
}

pub fn client_topic_alias_can_be_remapped() {
    let broker = broker_or_skip!();
    let t = topic();
    let (a, b) = (format!("{t}/a"), format!("{t}/b"));
    let mut sub = session(&broker);
    sub.subscribe(&format!("{t}/+"), 0).unwrap();
    let (mut c, ack) = session_with_ack(&broker);
    if server_alias_max(&ack).is_none() {
        return;
    }
    for (topic_name, payload) in [(&a, b"1"), (&b, b"2")] {
        let mut set = Publish::new(topic_name, payload);
        set.properties = vec![alias(1)];
        c.publish_packet(set).unwrap();
        let mut by_alias = Publish::new("", payload);
        by_alias.properties = vec![alias(1)];
        c.publish_packet(by_alias).unwrap();
    }
    // Order is guaranteed only per topic (MQTT-4.6.0-5), so compare
    // (topic, payload) pairs as a multiset.
    let mut got: Vec<(String, Vec<u8>)> = sub
        .expect_publishes(4, default_timeout())
        .unwrap()
        .into_iter()
        .map(|p| (p.topic, p.payload))
        .collect();
    got.sort();
    let one = || (a.clone(), b"1".to_vec());
    let two = || (b.clone(), b"2".to_vec());
    // req: 5.0/MQTT-3.3.2-12
    assert_eq!(got, [one(), one(), two(), two()]);
}

pub fn unmapped_topic_alias_is_a_protocol_error() {
    let broker = broker_or_skip!();
    let (mut c, ack) = session_with_ack(&broker);
    if server_alias_max(&ack).is_none() {
        return;
    }
    let mut p = Publish::new("", b"who?");
    p.properties = vec![alias(1)];
    c.send(&Packet::Publish(p)).unwrap();
    // An empty Topic Name with an alias that has no mapping is a Protocol
    // Error (3.3.2.3.4).
    // req: 5.0/MQTT-4.13.1-1
    // Only the close is required; any error code from the DISCONNECT table
    // may accompany it (0x82 Protocol Error is the obvious one).
    c.expect_error_close(default_timeout()).unwrap();
}

pub fn topic_alias_mappings_do_not_survive_the_connection() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("tal");
    let expiry = PropValue::FourByte(super::SESSION_EXPIRY);
    let (mut c, ack) = broker
        .connect_ok(
            &Connect::new(V, &id)
                .clean(false)
                .property(prop::SESSION_EXPIRY_INTERVAL, expiry.clone()),
        )
        .unwrap();
    if server_alias_max(&ack).is_none() {
        return;
    }
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let mut set = Publish::new(&t, b"set");
    set.properties = vec![alias(1)];
    c.publish_packet(set).unwrap();
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().payload,
        b"set"
    );
    c.disconnect().unwrap();
    let (mut c, _) = broker
        .connect_ok(
            &Connect::new(V, &id)
                .clean(false)
                .property(prop::SESSION_EXPIRY_INTERVAL, expiry),
        )
        .unwrap();
    let mut p = Publish::new("", b"stale");
    p.properties = vec![alias(1)];
    c.send(&Packet::Publish(p)).unwrap();
    // The close an unmapped alias calls for is checked by
    // unmapped_topic_alias_is_a_protocol_error; here the old mapping must not
    // be used: nothing reaches the topic it named.
    let _ = c.packets_until_close(default_timeout());
    // req: 5.0/MQTT-3.3.2-7
    sub.expect_no_packet(quiet_period()).unwrap();
    super::forget(&broker, &id);
}

/// Sends a QoS 0 PUBLISH with Topic Alias `n` on a fresh connection and
/// returns the DISCONNECT reason code (if any) of the required error close.
fn alias_error(broker: &crate::Broker, n: u16) -> Option<u8> {
    let mut c = session(broker);
    let mut p = Publish::new(&topic(), b"x");
    p.properties = vec![alias(n)];
    c.send(&Packet::Publish(p)).unwrap();
    c.expect_error_close(default_timeout()).unwrap()
}

/// Strict: MQTT-3.3.2-9 binds the client; closing relies on detection (MQTT-4.13.1-1).
pub fn topic_alias_above_the_maximum_is_disconnected_with_0x94() {
    let broker = broker_or_skip!();
    let (_, ack) = session_with_ack(&broker);
    let max = u16::try_from(connack_u32(&ack, prop::TOPIC_ALIAS_MAXIMUM, 0)).unwrap();
    let Some(above) = max.checked_add(1) else {
        crate::outcome::skipped("Topic Alias Maximum is 65535");
        return;
    };
    // req: 5.0/MQTT-3.3.2-9
    let reason = alias_error(&broker, above);
    // DISCONNECT is optional; if sent it must be 0x94 (Topic Alias invalid),
    // the code defined for exactly this case.
    if let Some(r) = reason {
        // req: 5.0/MQTT-3.3.2-9
        assert_eq!(r, 0x94, "alias {above} with maximum {max}");
    }
}

/// Strict: MQTT-3.3.2-8 binds the sender; closing relies on detection (MQTT-4.13.1-1).
pub fn topic_alias_zero_is_disconnected_with_0x94_or_0x82() {
    let broker = broker_or_skip!();
    // req: 5.0/MQTT-3.3.2-8
    let reason = alias_error(&broker, 0);
    // "A Topic Alias value of 0 … is a Protocol Error, the receiver uses
    // DISCONNECT with Reason Code of 0x94" (§3.3.4); 0x82 (Protocol Error)
    // is accepted too, as the generic code for a Protocol Error.
    if let Some(r) = reason {
        // req: 5.0/MQTT-3.3.2-8
        assert!(matches!(r, 0x82 | 0x94), "alias 0: reason {r:#04x}");
    }
}

/// Resolves server-to-client topic aliases like a client would, checking
/// each against `max`. Returns the resolved topic names.
fn resolve(publishes: &[Publish], max: u16) -> Vec<String> {
    let mut map: HashMap<u16, String> = HashMap::new();
    publishes
        .iter()
        .map(|p| match find_prop(&p.properties, prop::TOPIC_ALIAS) {
            None => {
                assert!(!p.topic.is_empty(), "empty topic without alias: {p:?}");
                p.topic.clone()
            }
            Some(PropValue::TwoByte(a)) => {
                // req: 5.0/MQTT-3.1.2-26
                // req: 5.0/MQTT-3.3.2-11
                // req: 5.0/MQTT-3.3.2-8
                assert!((1..=max).contains(a), "alias {a} with maximum {max}");
                if p.topic.is_empty() {
                    map.get(a)
                        .unwrap_or_else(|| panic!("unmapped alias {a}"))
                        .clone()
                } else {
                    map.insert(*a, p.topic.clone());
                    p.topic.clone()
                }
            }
            Some(other) => panic!("Topic Alias of type {other:?}"),
        })
        .collect()
}

pub fn server_topic_aliases_respect_the_clients_maximum() {
    let broker = broker_or_skip!();
    let t = topic();
    let (mut sub, _) = broker
        .connect_ok(
            &Connect::new(V, &unique_id("sta"))
                .property(prop::TOPIC_ALIAS_MAXIMUM, PropValue::TwoByte(2)),
        )
        .unwrap();
    sub.subscribe(&format!("{t}/+"), 0).unwrap();
    let mut publisher = session(&broker);
    let names: Vec<String> = (0..9).map(|i| format!("{t}/{}", i % 3)).collect();
    for n in &names {
        publisher.publish(n, b"a", 0, false).unwrap();
    }
    let got = sub
        .expect_publishes(names.len(), default_timeout())
        .unwrap();
    // Resolve in arrival order (aliases are per connection, in order), then
    // compare as a multiset: order is guaranteed only per topic.
    let mut resolved = resolve(&got, 2);
    resolved.sort();
    let mut expected = names;
    expected.sort();
    assert_eq!(resolved, expected);
}

pub fn no_topic_aliases_without_a_client_topic_alias_maximum() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let mut publisher = session(&broker);
    for _ in 0..5 {
        publisher.publish(&t, b"a", 0, false).unwrap();
    }
    for p in sub.expect_publishes(5, default_timeout()).unwrap() {
        // req: 5.0/MQTT-3.1.2-27
        assert_eq!(find_prop(&p.properties, prop::TOPIC_ALIAS), None, "{p:?}");
        assert_eq!(p.topic, t);
    }
}

/// Strict: MQTT-3.2.2-18 binds the client; closing relies on detection (MQTT-4.13.1-1).
pub fn topic_alias_without_a_server_topic_alias_maximum_is_an_error() {
    let broker = broker_or_skip!();
    let (mut c, ack) = session_with_ack(&broker);
    let max = connack_u32(&ack, prop::TOPIC_ALIAS_MAXIMUM, 0);
    if max != 0 {
        crate::outcome::skipped(format!("broker accepts Topic Aliases (maximum {max})"));
        return;
    }
    let mut p = Publish::new(&topic(), b"x");
    p.properties = vec![alias(1)];
    c.send(&Packet::Publish(p)).unwrap();
    // req: 5.0/MQTT-3.2.2-18
    let reason = c.expect_error_close(default_timeout()).unwrap();
    if let Some(r) = reason {
        // req: 5.0/MQTT-3.2.2-18
        assert_eq!(r, 0x94, "Topic Alias invalid");
    }
}

/// Fails unless the packet `bytes` fits the client's Maximum Packet Size.
fn fits(bytes: &[u8], max: usize, what: &str) {
    assert!(
        bytes.len() <= max,
        "{what} of {} bytes exceeds Maximum Packet Size {max}: {bytes:02x?}",
        bytes.len()
    );
}

/// The next packet on `c`, which must be `kind`, and its bytes.
fn raw_expect(c: &mut super::Raw, kind: &str) -> (Packet, Vec<u8>) {
    let (p, bytes) = c.frame();
    assert_eq!(p.name(), kind, "{p:?}");
    (p, bytes)
}

pub fn server_omits_reason_strings_and_user_properties_that_do_not_fit() {
    let broker = broker_or_skip!();
    // The raw client below bypasses the harness's capability gate, and the
    // case publishes at QoS 2.
    let (_, caps) = session_with_ack(&broker);
    if connack_u32(&caps, prop::MAXIMUM_QOS, 2) < 2 {
        crate::outcome::skipped("broker announces Maximum QoS below 2");
        return;
    }
    let t = unique_id("mps");
    // Smallest Maximum Packet Size that leaves room for this broker's
    // CONNACK without Reason String and User Properties and for the one
    // PUBLISH delivered below. Any optional Reason String or User Property
    // on top of that is likely to overflow it.
    let (_, probe) = session_with_ack(&broker);
    let mut bare = probe.clone();
    bare.properties
        .retain(|p| p.id != prop::REASON_STRING && p.id != prop::USER_PROPERTY);
    let mut delivered = Publish::new(&t, b"x");
    delivered.qos = 2;
    delivered.packet_id = Some(1);
    let max = Packet::ConnAck(bare)
        .encode(V)
        .len()
        .max(Packet::Publish(delivered).encode(V).len())
        .max(16);
    let connect = Connect::new(V, &unique_id("mps")).property(
        prop::MAXIMUM_PACKET_SIZE,
        PropValue::FourByte(u32::try_from(max).unwrap()),
    );
    let mut c = super::Raw::connect(&broker, &connect);
    let (ack, bytes) = raw_expect(&mut c, "CONNACK");
    assert!(
        matches!(ack, Packet::ConnAck(ref a) if a.code == 0),
        "{ack:?}"
    );
    // req: 5.0/MQTT-3.2.2-19
    // req: 5.0/MQTT-3.2.2-20
    fits(&bytes, max, "CONNACK");

    c.send(&Packet::Subscribe(crate::Subscribe {
        packet_id: 1,
        properties: Vec::new(),
        filters: vec![(t.clone(), 2)],
    }));
    let (_, bytes) = raw_expect(&mut c, "SUBACK");
    // req: 5.0/MQTT-3.9.2-1
    // req: 5.0/MQTT-3.9.2-2
    fits(&bytes, max, "SUBACK");

    // Unsubscribing a filter with no subscription (0x11 or 0x00).
    c.send(&Packet::Unsubscribe(crate::Unsubscribe {
        packet_id: 2,
        properties: Vec::new(),
        filters: vec![format!("{t}/none")],
    }));
    let (_, bytes) = raw_expect(&mut c, "UNSUBACK");
    // req: 5.0/MQTT-3.11.2-1
    // req: 5.0/MQTT-3.11.2-2
    fits(&bytes, max, "UNSUBACK");

    // Publishing where nobody listens (0x10 or 0x00).
    let mut p = Publish::new(&format!("{t}/nobody"), b"x");
    p.qos = 1;
    p.packet_id = Some(3);
    c.send(&Packet::Publish(p.clone()));
    let (_, bytes) = raw_expect(&mut c, "PUBACK");
    // req: 5.0/MQTT-3.4.2-2
    // req: 5.0/MQTT-3.4.2-3
    fits(&bytes, max, "PUBACK");
    p.qos = 2;
    p.packet_id = Some(4);
    c.send(&Packet::Publish(p));
    let (_, bytes) = raw_expect(&mut c, "PUBREC");
    // req: 5.0/MQTT-3.5.2-2
    // req: 5.0/MQTT-3.5.2-3
    fits(&bytes, max, "PUBREC");
    c.send(&Packet::PubRel(Ack::new(4)));
    let (_, bytes) = raw_expect(&mut c, "PUBCOMP");
    // req: 5.0/MQTT-3.7.2-2
    // req: 5.0/MQTT-3.7.2-3
    fits(&bytes, max, "PUBCOMP");
    // PUBREL for an unknown identifier (PUBCOMP 0x92 or 0x00).
    c.send(&Packet::PubRel(Ack::new(999)));
    let (_, bytes) = raw_expect(&mut c, "PUBCOMP");
    // req: 5.0/MQTT-3.7.2-2
    // req: 5.0/MQTT-3.7.2-3
    fits(&bytes, max, "PUBCOMP for an unknown identifier");

    // A QoS 2 delivery to this client: the broker's PUBREL.
    let mut publisher = session(&broker);
    publisher.publish(&t, b"x", 2, false).unwrap();
    let (p, _) = raw_expect(&mut c, "PUBLISH");
    let Packet::Publish(p) = p else {
        unreachable!()
    };
    // The broker may have granted less than QoS 2 (3.8.4): then there is no
    // PUBREL to measure.
    match (p.qos, p.packet_id) {
        (2, Some(id)) => {
            c.send(&Packet::PubRec(Ack::new(id)));
            let (_, bytes) = raw_expect(&mut c, "PUBREL");
            // req: 5.0/MQTT-3.6.2-2
            // req: 5.0/MQTT-3.6.2-3
            fits(&bytes, max, "PUBREL");
            c.send(&Packet::PubComp(Ack::new(id)));
        }
        (q, id) => {
            crate::outcome::note(format!("delivered at QoS {q}; no PUBREL to check"));
            if let (1, Some(id)) = (q, id) {
                c.send(&Packet::PubAck(Ack::new(id)));
            }
        }
    }

    // A malformed PUBLISH (QoS 3, MQTT-3.3.1-4): any DISCONNECT must fit too.
    let mut bad = Publish::new(&t, b"x");
    bad.qos = 3;
    bad.packet_id = Some(5);
    c.send(&Packet::Publish(bad));
    loop {
        match c.next(default_timeout()) {
            super::RawEvent::Frame(p, bytes) => {
                assert_eq!(p.name(), "DISCONNECT", "{p:?}");
                // req: 5.0/MQTT-3.14.2-3
                // req: 5.0/MQTT-3.14.2-4
                fits(&bytes, max, "DISCONNECT");
            }
            super::RawEvent::Closed => break,
            super::RawEvent::Timeout => panic!("connection still open after a QoS 3 PUBLISH"),
        }
    }
}

pub fn refusing_connack_fits_the_clients_maximum_packet_size() {
    let broker = broker_or_skip!();
    if !super::refuses_unknown_auth_method(&broker) {
        crate::outcome::skipped("the broker accepts an unknown Authentication Method: no refusal");
        return;
    }
    // A refusal needs only flags, reason code and an empty Property Length
    // (5 bytes); a Reason String or User Property would not fit in 8.
    let max = 8;
    let connect = Connect::new(V, &unique_id("mpr"))
        .property(prop::MAXIMUM_PACKET_SIZE, PropValue::FourByte(8))
        .property(
            prop::AUTHENTICATION_METHOD,
            PropValue::Str("x-mqtt-testkit-unknown".into()),
        );
    let mut c = super::Raw::connect(&broker, &connect);
    loop {
        match c.next(default_timeout()) {
            super::RawEvent::Frame(p, bytes) => {
                assert!(
                    matches!(p, Packet::ConnAck(ref a) if a.code >= 0x80),
                    "{p:?}"
                );
                // req: 5.0/MQTT-3.2.2-19
                // req: 5.0/MQTT-3.2.2-20
                fits(&bytes, max, "CONNACK");
            }
            super::RawEvent::Closed => break,
            super::RawEvent::Timeout => panic!("refused connection still open"),
        }
    }
}
