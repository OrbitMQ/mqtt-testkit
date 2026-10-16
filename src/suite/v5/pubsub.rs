// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! PUBLISH / SUBSCRIBE / UNSUBSCRIBE flows and QoS 0/1/2 delivery (MQTT 5.0).

use std::collections::HashSet;

use crate::{
    Ack, Client, Packet, PropValue, Property, Publish, Subscribe, Unsubscribe, broker_or_skip,
    default_timeout, prop, quiet_period,
};

use super::{session, topic};

pub fn qos0_message_is_delivered_with_qos0_and_dup_0() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"q0", 0, false).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-4.3.1-1
    // req: 5.0/MQTT-3.3.1-2
    assert_eq!((p.qos, p.dup, p.packet_id), (0, false, None), "{p:?}");
    // req: 5.0/MQTT-3.3.2-3
    assert_eq!(p.topic, t);
    assert_eq!(p.payload, b"q0");
}

pub fn qos1_publish_is_acknowledged_with_its_packet_identifier() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    let mut p = Publish::new(&topic(), b"q1");
    p.qos = 1;
    p.packet_id = Some(0x1234);
    // req: 5.0/MQTT-3.3.4-1
    // req: 5.0/MQTT-4.3.2-4
    let ack = c.publish_packet(p).unwrap().unwrap();
    assert_eq!(ack.packet_id, 0x1234);
}

pub fn qos2_publish_gets_pubrec_and_pubrel_gets_pubcomp() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    let mut p = Publish::new(&topic(), b"q2");
    p.qos = 2;
    p.packet_id = Some(0x0F0F);
    c.send(&Packet::Publish(p)).unwrap();
    // req: 5.0/MQTT-3.3.4-1
    // req: 5.0/MQTT-4.3.3-8
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRec(a) => assert_eq!(a.packet_id, 0x0F0F),
        other => panic!("expected PUBREC, got {other:?}"),
    }
    c.send(&Packet::PubRel(Ack::new(0x0F0F))).unwrap();
    // req: 5.0/MQTT-4.3.3-11
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubComp(a) => assert_eq!(a.packet_id, 0x0F0F),
        other => panic!("expected PUBCOMP, got {other:?}"),
    }
}

pub fn qos1_delivery_uses_a_packet_identifier_and_dup_0() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"one", 1, false).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-4.3.2-1
    // req: 5.0/MQTT-4.3.2-2
    assert_eq!((p.qos, p.dup), (1, false), "{p:?}");
    assert!(p.packet_id.is_some_and(|id| id != 0), "{p:?}");
    assert_eq!(p.payload, b"one");
}

pub fn qos2_delivery_follows_the_pubrec_pubrel_pubcomp_flow() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"two", 2, false).unwrap();
    let p = sub.expect_publish_unacked(default_timeout()).unwrap();
    // req: 5.0/MQTT-4.3.3-1
    // req: 5.0/MQTT-4.3.3-2
    assert_eq!((p.qos, p.dup), (2, false), "{p:?}");
    let id = p.packet_id.unwrap();
    assert_ne!(id, 0);
    sub.send(&Packet::PubRec(Ack::new(id))).unwrap();
    // req: 5.0/MQTT-4.3.3-4
    match sub.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(a) => assert_eq!(a.packet_id, id),
        other => panic!("expected PUBREL, got {other:?}"),
    }
    sub.send(&Packet::PubComp(Ack::new(id))).unwrap();
    // After PUBREL the PUBLISH must never be re-sent.
    // req: 5.0/MQTT-4.3.3-6
    sub.expect_no_packet(quiet_period()).unwrap();
}

/// Publishes at `pub_qos` to a subscription granted `sub_qos` and returns
/// the QoS of the delivered message.
fn delivered_qos(sub_qos: u8, pub_qos: u8) -> Option<u8> {
    let broker = crate::Broker::for_test()?;
    let t = topic();
    let mut sub = session(&broker);
    let ack = sub.subscribe(&t, sub_qos).unwrap();
    let granted = ack.codes[0];
    assert!(
        granted <= sub_qos,
        "granted {granted} > requested {sub_qos}"
    );
    let mut publisher = session(&broker);
    publisher.publish(&t, b"qos", pub_qos, false).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.8.4-8
    assert_eq!(
        p.qos,
        pub_qos.min(granted),
        "delivered at {} for publish {pub_qos} / granted {granted}",
        p.qos
    );
    Some(p.qos)
}

pub fn qos2_publish_is_downgraded_to_a_qos0_subscription() {
    // req: 5.0/MQTT-3.8.4-8
    if let Some(q) = delivered_qos(0, 2) {
        assert_eq!(q, 0);
    }
}

pub fn qos2_publish_is_downgraded_to_a_qos1_subscription() {
    // A server may grant less than requested; `delivered_qos` requires
    // exactly min(publish, granted).
    // req: 5.0/MQTT-3.8.4-8
    if let Some(q) = delivered_qos(1, 2) {
        assert!(q <= 1);
    }
}

pub fn qos1_publish_is_downgraded_to_a_qos0_subscription() {
    // req: 5.0/MQTT-3.8.4-8
    if let Some(q) = delivered_qos(0, 1) {
        assert_eq!(q, 0);
    }
}

pub fn qos0_publish_is_not_upgraded_by_a_qos2_subscription() {
    // req: 5.0/MQTT-3.8.4-8
    if let Some(q) = delivered_qos(2, 0) {
        assert_eq!(q, 0);
    }
}

pub fn overlapping_subscriptions_deliver_at_the_highest_qos() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    let a = sub.subscribe(&format!("{t}/+"), 0).unwrap();
    let b = sub.subscribe(&format!("{t}/x"), 2).unwrap();
    let mut publisher = session(&broker);
    publisher
        .publish(&format!("{t}/x"), b"o", 2, false)
        .unwrap();
    // One copy at the highest QoS, or one copy per subscription: both are
    // allowed. Either way some copy must carry the highest granted QoS.
    let got = sub.collect_publishes(quiet_period()).unwrap();
    assert!(!got.is_empty() && got.len() <= 2, "{got:?}");
    let max = got.iter().map(|p| p.qos).max().unwrap_or(0);
    // req: 5.0/MQTT-3.3.4-2
    assert_eq!(max, a.codes[0].max(b.codes[0]).min(2), "{got:?}");
}

pub fn suback_has_one_code_per_filter_in_order() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut c = session(&broker);
    let filters = [
        (format!("{t}/a"), 0u8),
        (format!("{t}/b"), 1),
        (format!("{t}/c"), 2),
    ];
    let refs: Vec<(&str, u8)> = filters.iter().map(|(f, q)| (f.as_str(), *q)).collect();
    let id = c.next_packet_id().wrapping_add(1);
    // req: 5.0/MQTT-3.8.4-1
    // req: 5.0/MQTT-3.8.4-2
    let ack = c.subscribe_many(&refs, Vec::new()).unwrap();
    assert_eq!(ack.packet_id, id);
    // req: 5.0/MQTT-3.8.4-5
    // req: 5.0/MQTT-3.8.4-6
    // req: 5.0/MQTT-3.8.4-7
    // req: 5.0/MQTT-3.9.3-1
    assert_eq!(ack.codes.len(), 3, "{ack:?}");
    for (code, (_, requested)) in ack.codes.iter().zip(&filters) {
        // req: 5.0/MQTT-3.9.3-2
        assert!(
            *code <= *requested || *code >= 0x80,
            "code {code:#04x} for requested QoS {requested}"
        );
    }
    // Every filter of the multi-filter SUBSCRIBE took effect.
    let mut publisher = session(&broker);
    for (f, _) in &filters {
        publisher.publish(f, b"m", 0, false).unwrap();
    }
    let got = c.expect_publishes(3, default_timeout()).unwrap();
    let topics: HashSet<_> = got.iter().map(|p| p.topic.clone()).collect();
    // req: 5.0/MQTT-3.8.4-5
    assert_eq!(topics.len(), 3, "{got:?}");
}

pub fn resubscribing_replaces_the_subscription() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let ack = sub.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"r", 2, false).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.8.4-3
    assert_eq!(p.qos, ack.codes[0].min(2), "QoS of replaced subscription");
    // Replaced, not added: exactly one copy.
    assert_eq!(sub.collect_publishes(quiet_period()).unwrap(), []);
}

pub fn unsubscribe_stops_delivery_and_is_acknowledged() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let id = sub.next_packet_id().wrapping_add(1);
    // req: 5.0/MQTT-3.10.4-4
    // req: 5.0/MQTT-3.10.4-5
    let ack = sub.unsubscribe(&t).unwrap();
    assert_eq!(ack.packet_id, id);
    let mut publisher = session(&broker);
    publisher.publish(&t, b"gone", 1, false).unwrap();
    // req: 5.0/MQTT-3.10.4-2
    sub.expect_no_packet(quiet_period()).unwrap();
}

pub fn unsubscribe_without_a_subscription_still_gets_unsuback() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    // req: 5.0/MQTT-3.10.4-5
    // req: 5.0/MQTT-3.10.4-4
    let ack = c.unsubscribe(&topic()).unwrap();
    // 0x11 (No subscription existed) is the precise code; 0x00 is tolerated.
    // req: 5.0/MQTT-3.11.3-2
    assert!(matches!(ack.codes.as_slice(), [0x00 | 0x11]), "{ack:?}");
}

pub fn unsubscribe_with_several_filters_gets_one_unsuback() {
    let broker = broker_or_skip!();
    let t = topic();
    let (a, b) = (format!("{t}/a"), format!("{t}/b"));
    let mut sub = session(&broker);
    sub.subscribe(&a, 0).unwrap();
    sub.subscribe(&b, 0).unwrap();
    // req: 5.0/MQTT-3.10.4-6
    sub.unsubscribe_many(&[&a, &b]).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&a, b"x", 0, false).unwrap();
    publisher.publish(&b, b"x", 0, false).unwrap();
    // req: 5.0/MQTT-3.10.4-6
    // req: 5.0/MQTT-3.10.4-2
    sub.expect_no_packet(quiet_period()).unwrap();
}

pub fn unsubscribe_compares_filters_literally() {
    let broker = broker_or_skip!();
    let t = topic();
    let wildcard = format!("{t}/+");
    let concrete = format!("{t}/x");
    let mut sub = session(&broker);
    sub.subscribe(&wildcard, 0).unwrap();
    // "t/x" matches "t/+" as a topic, but is not the same filter.
    sub.unsubscribe(&concrete).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&concrete, b"still", 0, false).unwrap();
    // req: 5.0/MQTT-3.10.4-1
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().payload,
        b"still"
    );
    sub.unsubscribe(&wildcard).unwrap();
    publisher.publish(&concrete, b"gone", 0, false).unwrap();
    // req: 5.0/MQTT-3.10.4-1
    sub.expect_no_packet(quiet_period()).unwrap();
}

pub fn qos1_packet_identifier_can_be_reused_after_puback() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    for (payload, dup) in [(&b"first"[..], false), (b"second", true)] {
        let mut p = Publish::new(&t, payload);
        p.qos = 1;
        p.dup = dup;
        p.packet_id = Some(7);
        publisher.publish_packet(p).unwrap();
    }
    // After PUBACK, a PUBLISH with the same identifier is a new message,
    // even with DUP set.
    let got = sub.expect_publishes(2, default_timeout()).unwrap();
    // req: 5.0/MQTT-4.3.2-4
    // req: 5.0/MQTT-4.3.2-5
    assert_eq!(got[0].payload, b"first");
    assert_eq!(got[1].payload, b"second");
}

pub fn qos2_duplicate_before_pubrel_is_not_delivered_twice() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    let mut p = Publish::new(&t, b"once");
    p.qos = 2;
    p.packet_id = Some(9);
    publisher.send(&Packet::Publish(p.clone())).unwrap();
    let rec = |c: &mut crate::Client| match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRec(a) => assert_eq!(a.packet_id, 9),
        other => panic!("expected PUBREC, got {other:?}"),
    };
    rec(&mut publisher);
    p.dup = true;
    publisher.send(&Packet::Publish(p)).unwrap();
    // req: 5.0/MQTT-4.3.3-10
    rec(&mut publisher);
    publisher.send(&Packet::PubRel(Ack::new(9))).unwrap();
    match publisher.expect_packet(default_timeout()).unwrap() {
        Packet::PubComp(a) => assert_eq!(a.packet_id, 9),
        other => panic!("expected PUBCOMP, got {other:?}"),
    }
    let got = sub.collect_publishes(quiet_period()).unwrap();
    // req: 5.0/MQTT-4.3.3-10
    assert_eq!(got.len(), 1, "{got:?}");
}

pub fn qos2_packet_identifier_is_new_after_pubcomp() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    for payload in [&b"a"[..], b"b"] {
        let mut p = Publish::new(&t, payload);
        p.qos = 2;
        p.packet_id = Some(11);
        publisher.publish_packet(p).unwrap();
    }
    let got = sub.expect_publishes(2, default_timeout()).unwrap();
    // req: 5.0/MQTT-4.3.3-12
    assert_eq!(got[0].payload, b"a");
    assert_eq!(got[1].payload, b"b");
}

pub fn inbound_dup_flag_is_not_propagated() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    let mut p = Publish::new(&t, b"dup");
    p.qos = 1;
    p.dup = true;
    publisher.publish_packet(p).unwrap();
    let got = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.3.1-3
    assert!(!got.dup, "{got:?}");
}

/// Publishes 20 messages at `qos` and checks they arrive in order. The
/// publisher pipelines up to the server's Receive Maximum and completes
/// every flow (PUBRELs in PUBREC order), since a server may forward a QoS 2
/// message only on PUBREL.
fn ordered(qos: u8) {
    let Some(broker) = crate::Broker::for_test() else {
        return;
    };
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, qos).unwrap();
    let (mut publisher, ack) = super::session_with_ack(&broker);
    let window = usize::try_from(super::connack_u32(&ack, prop::RECEIVE_MAXIMUM, 65_535))
        .unwrap()
        .max(1);
    let n = 20u8;
    let messages: Vec<u8> = (0..n).collect();
    for batch in messages.chunks(if qos == 0 { messages.len() } else { window }) {
        let mut ids = Vec::new();
        for &i in batch {
            let mut p = Publish::new(&t, &[i]);
            p.qos = qos;
            if qos > 0 {
                let id = publisher.next_packet_id();
                p.packet_id = Some(id);
                ids.push(id);
            }
            // Don't wait for acknowledgements: pipeline the batch.
            publisher.send(&Packet::Publish(p)).unwrap();
        }
        match qos {
            0 => {}
            1 => {
                for _ in &ids {
                    ack_of(&mut publisher, "PUBACK");
                }
            }
            _ => {
                let recs: Vec<u16> = ids
                    .iter()
                    .map(|_| ack_of(&mut publisher, "PUBREC").packet_id)
                    .collect();
                for id in &recs {
                    publisher.send(&Packet::PubRel(Ack::new(*id))).unwrap();
                }
                for _ in &recs {
                    ack_of(&mut publisher, "PUBCOMP");
                }
            }
        }
    }
    let got = sub
        .expect_publishes(usize::from(n), default_timeout())
        .unwrap();
    let order: Vec<u8> = got.iter().map(|p| p.payload[0]).collect();
    // req: 5.0/MQTT-4.6.0-5
    // req: 5.0/MQTT-4.6.0-6
    assert_eq!(order, messages);
}

pub fn qos0_messages_from_one_publisher_keep_their_order() {
    ordered(0);
}

pub fn qos1_messages_from_one_publisher_keep_their_order() {
    ordered(1);
}

pub fn qos2_messages_from_one_publisher_keep_their_order() {
    ordered(2);
}

pub fn in_flight_deliveries_use_distinct_packet_identifiers() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    for i in 0..5u8 {
        publisher.publish(&t, &[i], 1, false).unwrap();
    }
    let mut ids = HashSet::new();
    for _ in 0..5 {
        // Not acknowledged, so every identifier stays in use.
        let p = sub.expect_publish_unacked(default_timeout()).unwrap();
        let id = p.packet_id.unwrap();
        // req: 5.0/MQTT-4.3.2-1
        // req: 5.0/MQTT-4.3.2-2
        // req: 5.0/MQTT-2.2.1-4
        assert!(
            id != 0 && ids.insert(id),
            "reused or zero id {id} in {ids:?}"
        );
    }
}

pub fn message_reaches_every_matching_subscriber() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut subs: Vec<_> = (0..3).map(|_| session(&broker)).collect();
    for (i, s) in subs.iter_mut().enumerate() {
        let filter = match i {
            0 => t.clone(),
            1 => format!("{t}/#"),
            _ => "conformance/+".to_owned(),
        };
        s.subscribe(&filter, 1).unwrap();
    }
    let mut publisher = session(&broker);
    publisher.publish(&t, b"all", 1, false).unwrap();
    for s in &mut subs {
        // req: 5.0/MQTT-4.5.0-1
        let p = s.expect_publish(default_timeout()).unwrap();
        // With a shared broker "conformance/+" can see other tests' traffic.
        if p.topic == t {
            assert_eq!(p.payload, b"all");
        }
    }
}

pub fn zero_length_payload_is_delivered() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"", 0, false).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    assert_eq!(p.payload, b"");
}

pub fn subscribe_with_duplicate_filters_in_one_packet_is_handled_sequentially() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut c = session(&broker);
    let id = c.next_packet_id();
    c.send(&Packet::Subscribe(Subscribe {
        packet_id: id,
        properties: Vec::new(),
        filters: vec![(t.clone(), 0), (t.clone(), 1)],
    }))
    .unwrap();
    let ack = match c.expect_packet(default_timeout()).unwrap() {
        Packet::SubAck(s) => s,
        other => panic!("expected SUBACK, got {other:?}"),
    };
    // req: 5.0/MQTT-3.8.4-6
    // req: 5.0/MQTT-3.8.4-7
    assert_eq!(ack.codes.len(), 2, "{ack:?}");
    let mut publisher = session(&broker);
    publisher.publish(&t, b"seq", 1, false).unwrap();
    // The second filter replaced the first: one copy, at the second QoS.
    let got = c.collect_publishes(quiet_period()).unwrap();
    // req: 5.0/MQTT-3.8.4-5
    // req: 5.0/MQTT-3.8.4-3
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0].qos, ack.codes[1].min(1));
}

/// The next packet on `c`, which must be the acknowledgement `kind`
/// (PUBACK, PUBREC, PUBREL or PUBCOMP); returns it.
fn ack_of(c: &mut Client, kind: &str) -> Ack {
    match (kind, c.expect_packet(default_timeout()).unwrap()) {
        ("PUBACK", Packet::PubAck(a))
        | ("PUBREC", Packet::PubRec(a))
        | ("PUBREL", Packet::PubRel(a))
        | ("PUBCOMP", Packet::PubComp(a)) => a,
        (_, other) => panic!("expected {kind}, got {other:?}"),
    }
}

pub fn acknowledgements_echo_the_packet_identifier() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    for id in [0xFFFF, 0x8000, 0x0100] {
        let mut p = Publish::new(&t, b"q1");
        p.qos = 1;
        p.packet_id = Some(id);
        publisher.send(&Packet::Publish(p)).unwrap();
        // req: 5.0/MQTT-2.2.1-5
        assert_eq!(ack_of(&mut publisher, "PUBACK").packet_id, id);
        let mut p = Publish::new(&t, b"q2");
        p.qos = 2;
        p.packet_id = Some(id);
        publisher.send(&Packet::Publish(p)).unwrap();
        // req: 5.0/MQTT-2.2.1-5
        assert_eq!(ack_of(&mut publisher, "PUBREC").packet_id, id);
        publisher.send(&Packet::PubRel(Ack::new(id))).unwrap();
        // req: 5.0/MQTT-2.2.1-5
        assert_eq!(ack_of(&mut publisher, "PUBCOMP").packet_id, id);
    }
    // Server-sent flow: its PUBREL echoes the identifier of its PUBLISH.
    for _ in 0..6 {
        let p = sub.expect_publish_unacked(default_timeout()).unwrap();
        let id = p.packet_id.unwrap();
        if p.qos == 1 {
            sub.send(&Packet::PubAck(Ack::new(id))).unwrap();
            continue;
        }
        sub.send(&Packet::PubRec(Ack::new(id))).unwrap();
        // Later PUBLISHes may overtake the PUBREL; they are stashed. Only
        // this QoS 2 flow is open, so the PUBREL must carry its identifier.
        let rel = sub
            .wait_for(default_timeout(), "PUBREL", |p| match p {
                Packet::PubRel(a) => Some(a.packet_id),
                _ => None,
            })
            .unwrap();
        // req: 5.0/MQTT-2.2.1-5
        assert_eq!(rel, id);
        sub.send(&Packet::PubComp(Ack::new(id))).unwrap();
    }
}

pub fn suback_and_unsuback_echo_the_packet_identifier() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut c = session(&broker);
    for id in [0xFFFF, 0x8000, 0x0100, 1] {
        c.send(&Packet::Subscribe(Subscribe {
            packet_id: id,
            properties: Vec::new(),
            filters: vec![(t.clone(), 1)],
        }))
        .unwrap();
        match c.expect_packet(default_timeout()).unwrap() {
            // req: 5.0/MQTT-2.2.1-6
            Packet::SubAck(a) => assert_eq!(a.packet_id, id, "{a:?}"),
            other => panic!("expected SUBACK, got {other:?}"),
        }
        c.send(&Packet::Unsubscribe(Unsubscribe {
            packet_id: id,
            properties: Vec::new(),
            filters: vec![t.clone()],
        }))
        .unwrap();
        match c.expect_packet(default_timeout()).unwrap() {
            // req: 5.0/MQTT-2.2.1-6
            Packet::UnsubAck(a) => assert_eq!(a.packet_id, id, "{a:?}"),
            other => panic!("expected UNSUBACK, got {other:?}"),
        }
    }
}

pub fn qos2_identifier_stays_in_use_until_pubcomp() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"held", 2, false).unwrap();
    let held = sub.expect_publish_unacked(default_timeout()).unwrap();
    let held_id = held.packet_id.unwrap();
    sub.send(&Packet::PubRec(Ack::new(held_id))).unwrap();
    assert_eq!(ack_of(&mut sub, "PUBREL").packet_id, held_id);
    // No PUBCOMP yet: `held_id` is still in use for this session.
    for i in 0..5u8 {
        publisher.publish(&t, &[i], 2, false).unwrap();
    }
    let mut ids = HashSet::from([held_id]);
    for _ in 0..5 {
        let p = sub.expect_publish_unacked(default_timeout()).unwrap();
        let id = p.packet_id.unwrap();
        // req: 5.0/MQTT-2.2.1-4
        assert!(
            id != 0 && ids.insert(id),
            "reused or zero id {id} in {ids:?}"
        );
    }
    sub.send(&Packet::PubComp(Ack::new(held_id))).unwrap();
}

pub fn qos2_exchange_completes_after_message_expiry() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    let mut p = Publish::new(&t, b"short-lived");
    p.qos = 2;
    p.packet_id = Some(5);
    p.properties = vec![Property::new(
        prop::MESSAGE_EXPIRY_INTERVAL,
        PropValue::FourByte(1),
    )];
    publisher.send(&Packet::Publish(p)).unwrap();
    assert_eq!(ack_of(&mut publisher, "PUBREC").packet_id, 5);
    std::thread::sleep(std::time::Duration::from_millis(2_100));
    publisher.send(&Packet::PubRel(Ack::new(5))).unwrap();
    let comp = ack_of(&mut publisher, "PUBCOMP");
    // The identifier is still known after expiry, so the exchange completes
    // normally (0x92 would mean its state was discarded).
    // req: 5.0/MQTT-4.3.3-13
    assert_eq!(comp.packet_id, 5);
    // req: 5.0/MQTT-4.3.3-13
    assert_eq!(comp.reason.unwrap_or(0), 0x00, "{comp:?}");
}

pub fn identifier_rejected_by_an_error_pubrec_starts_a_new_message() {
    let broker = broker_or_skip!();
    let t = topic();
    // PUBLISHes a server may refuse with PUBREC >= 0x80: Payload Format
    // Indicator 1 with an ill-formed UTF-8 payload (0x99), and a topic
    // starting with "$SYS" (0x90 / 0x87 on many servers).
    let mut bad_payload = Publish::new(&t, &[0xC3, 0x28]);
    bad_payload.properties = vec![Property::new(
        prop::PAYLOAD_FORMAT_INDICATOR,
        PropValue::Byte(1),
    )];
    let sys = Publish::new(&format!("$SYS/{t}"), b"x");
    for mut rejected in [bad_payload, sys] {
        let mut c = session(&broker);
        rejected.qos = 2;
        rejected.packet_id = Some(77);
        c.send(&Packet::Publish(rejected)).unwrap();
        let rec = match c.expect_packet(default_timeout()) {
            Ok(Packet::PubRec(a)) => a,
            Ok(Packet::Disconnect(_)) | Err(crate::HarnessError::Closed(_)) => continue,
            Ok(other) => panic!("expected PUBREC, got {other:?}"),
            Err(e) => panic!("{e}"),
        };
        assert_eq!(rec.packet_id, 77);
        if rec.reason.unwrap_or(0) < 0x80 {
            c.send(&Packet::PubRel(Ack::new(77))).unwrap();
            assert_eq!(ack_of(&mut c, "PUBCOMP").packet_id, 77);
            continue;
        }
        // Refused: identifier 77 is free again, so a new PUBLISH with it is
        // a new message (delivered once, after its own PUBREL).
        let mut sub = session(&broker);
        sub.subscribe(&t, 2).unwrap();
        let mut fresh = Publish::new(&t, b"fresh");
        fresh.qos = 2;
        fresh.packet_id = Some(77);
        c.send(&Packet::Publish(fresh)).unwrap();
        let rec = ack_of(&mut c, "PUBREC");
        // req: 5.0/MQTT-4.3.3-9
        assert!(
            rec.packet_id == 77 && rec.reason.unwrap_or(0) < 0x80,
            "{rec:?}"
        );
        c.send(&Packet::PubRel(Ack::new(77))).unwrap();
        assert_eq!(ack_of(&mut c, "PUBCOMP").packet_id, 77);
        let got = sub.collect_publishes(quiet_period()).unwrap();
        // req: 5.0/MQTT-4.3.3-9
        assert_eq!(
            got.iter().map(|p| p.payload.clone()).collect::<Vec<_>>(),
            [b"fresh".to_vec()]
        );
        return;
    }
    crate::outcome::skipped("broker accepted every PUBLISH this case can get refused with PUBREC");
}
