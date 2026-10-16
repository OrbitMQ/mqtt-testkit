// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! PUBLISH / SUBSCRIBE / UNSUBSCRIBE flows and QoS 0/1/2 delivery (MQTT 3.1).

use std::collections::HashSet;

use crate::{
    Ack, Event, Packet, Publish, broker_or_skip, default_timeout, quiet_period, unique_id,
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
    // req: 3.1/V31-4.1-1
    assert_eq!((p.qos, p.dup, p.packet_id), (0, false, None), "{p:?}");
    // req: 3.1/V31-3.3-2
    assert_eq!(p.topic, t);
    assert_eq!(p.payload, b"q0");
}

pub fn qos1_publish_is_acknowledged_with_its_packet_identifier() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    let mut p = Publish::new(&topic(), b"q1");
    p.qos = 1;
    p.packet_id = Some(0x1234);
    // req: 3.1/V31-3.3-4
    // req: 3.1/V31-3.4-1
    // req: 3.1/V31-3.3-5
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
    // req: 3.1/V31-3.3-4
    // req: 3.1/V31-3.6-2
    // req: 3.1/V31-3.7-1
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRec(a) => assert_eq!(a.packet_id, 0x0F0F),
        other => panic!("expected PUBREC, got {other:?}"),
    }
    c.send(&Packet::PubRel(Ack::new(0x0F0F))).unwrap();
    // req: 3.1/V31-3.6-2
    // req: 3.1/V31-3.7-1
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
    // req: 3.1/V31-2.4-2
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
    // req: 3.1/V31-3.5-1
    // req: 3.1/V31-4.1-3
    assert_eq!((p.qos, p.dup), (2, false), "{p:?}");
    let id = p.packet_id.unwrap();
    // req: 3.1/V31-2.4-3
    assert_ne!(id, 0);
    sub.send(&Packet::PubRec(Ack::new(id))).unwrap();
    // req: 3.1/V31-3.5-1
    // req: 3.1/V31-4.1-3
    match sub.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(a) => assert_eq!(a.packet_id, id),
        other => panic!("expected PUBREL, got {other:?}"),
    }
    sub.send(&Packet::PubComp(Ack::new(id))).unwrap();
    // After PUBREL the PUBLISH must never be re-sent.
    // req: 3.1/V31-3.5-1
    // req: 3.1/V31-4.1-3
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
    assert_eq!(
        p.qos,
        pub_qos.min(granted),
        "delivered QoS for publish {pub_qos} / granted {granted}"
    );
    Some(p.qos)
}

pub fn qos2_publish_is_downgraded_to_a_qos0_subscription() {
    // req: 3.1/V31-3.8-3
    if let Some(q) = delivered_qos(0, 2) {
        assert_eq!(q, 0);
    }
}

pub fn qos2_publish_is_downgraded_to_a_qos1_subscription() {
    // A server may grant less than requested; `delivered_qos` checks
    // min(publish, granted). If QoS 1 was granted the delivery must be QoS 1.
    // req: 3.1/V31-3.8-3
    if let Some(q) = delivered_qos(1, 2) {
        assert!(q <= 1);
    }
}

pub fn qos1_publish_is_downgraded_to_a_qos0_subscription() {
    // req: 3.1/V31-3.8-3
    if let Some(q) = delivered_qos(0, 1) {
        assert_eq!(q, 0);
    }
}

pub fn qos0_publish_is_not_upgraded_by_a_qos2_subscription() {
    // req: 3.1/V31-3.8-3
    if let Some(q) = delivered_qos(2, 0) {
        assert_eq!(q, 0);
    }
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
    // req: 3.1/V31-3.8-5
    // req: 3.1/V31-3.9-1
    let ack = c.subscribe_many(&refs, Vec::new()).unwrap();
    assert_eq!(ack.packet_id, id);
    // req: 3.1/V31-3.9-2
    assert_eq!(ack.codes.len(), 3, "{ack:?}");
    for (code, (_, requested)) in ack.codes.iter().zip(&filters) {
        // req: 3.1/V31-3.9-3
        // 3.1 has no failure code: every code is a granted QoS <= requested.
        assert!(
            *code <= *requested,
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
    // The contiguous list of filter/QoS pairs was parsed in full.
    // req: 3.1/V31-3.8-2
    assert_eq!(topics.len(), 3, "{got:?}");
}

pub fn unsubscribe_stops_delivery_and_is_acknowledged() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let id = sub.next_packet_id().wrapping_add(1);
    // req: 3.1/V31-3.10-3
    // req: 3.1/V31-3.11-1
    let ack = sub.unsubscribe(&t).unwrap();
    assert_eq!(ack.packet_id, id);
    let mut publisher = session(&broker);
    publisher.publish(&t, b"gone", 1, false).unwrap();
    // req: 3.1/V31-3.10-4
    sub.expect_no_packet(quiet_period()).unwrap();
}

pub fn unsubscribe_without_a_subscription_still_gets_unsuback() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    // req: 3.1/V31-3.10-3
    // req: 3.1/V31-3.11-1
    c.unsubscribe(&topic()).unwrap();
}

pub fn unsubscribe_with_several_filters_gets_one_unsuback() {
    let broker = broker_or_skip!();
    let t = topic();
    let (a, b) = (format!("{t}/a"), format!("{t}/b"));
    let mut sub = session(&broker);
    sub.subscribe(&a, 0).unwrap();
    sub.subscribe(&b, 0).unwrap();
    // req: 3.1/V31-3.10-4
    sub.unsubscribe_many(&[&a, &b]).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&a, b"x", 0, false).unwrap();
    publisher.publish(&b, b"x", 0, false).unwrap();
    // req: 3.1/V31-3.10-4
    // req: 3.1/V31-3.10-2
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
    // req: 3.1/V31-3.10-4
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().payload,
        b"still"
    );
    sub.unsubscribe(&wildcard).unwrap();
    publisher.publish(&concrete, b"gone", 0, false).unwrap();
    // req: 3.1/V31-3.10-4
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
    // req: 3.1/V31-3.4-1
    // req: 3.1/V31-3.3-5
    // req: 3.1/V31-4.1-2
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
    rec(&mut publisher);
    publisher.send(&Packet::PubRel(Ack::new(9))).unwrap();
    match publisher.expect_packet(default_timeout()).unwrap() {
        Packet::PubComp(a) => assert_eq!(a.packet_id, 9),
        other => panic!("expected PUBCOMP, got {other:?}"),
    }
    let got = sub.collect_publishes(quiet_period()).unwrap();
    // req: 3.1/V31-4.1-3
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
    // req: 3.1/V31-3.6-2
    // req: 3.1/V31-3.7-1
    assert_eq!(got[0].payload, b"a");
    assert_eq!(got[1].payload, b"b");
}

/// Completes the publisher side of `n` pipelined PUBLISH packets at `qos`:
/// waits for each PUBACK, or answers each PUBREC with a PUBREL (in the
/// order the PUBRECs arrive) and waits for every PUBCOMP. A server may
/// forward a QoS 2 message only on PUBREL, so this must happen before the
/// deliveries are awaited.
fn complete_flows(publisher: &mut crate::Client, qos: u8, n: usize) {
    if qos == 0 {
        return;
    }
    let (mut acks, mut comps) = (0, 0);
    while acks < n || (qos == 2 && comps < n) {
        match publisher.expect_packet(default_timeout()).unwrap() {
            Packet::PubAck(_) if qos == 1 => acks += 1,
            Packet::PubRec(a) if qos == 2 => {
                publisher
                    .send(&Packet::PubRel(Ack::new(a.packet_id)))
                    .unwrap();
                acks += 1;
            }
            Packet::PubComp(_) if qos == 2 => comps += 1,
            other => panic!("QoS {qos} flow: unexpected {other:?}"),
        }
    }
}

/// Publishes 20 messages at `qos` and checks they arrive in order.
fn ordered(qos: u8) {
    let Some(broker) = crate::Broker::for_test() else {
        return;
    };
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, qos).unwrap();
    let mut publisher = session(&broker);
    let n = 20u8;
    for i in 0..n {
        let mut p = Publish::new(&t, &[i]);
        p.qos = qos;
        if qos > 0 {
            p.packet_id = Some(publisher.next_packet_id());
        }
        // Don't wait for acknowledgements: pipeline the messages.
        publisher.send(&Packet::Publish(p)).unwrap();
    }
    complete_flows(&mut publisher, qos, usize::from(n));
    let got = sub
        .expect_publishes(usize::from(n), default_timeout())
        .unwrap();
    let order: Vec<u8> = got.iter().map(|p| p.payload[0]).collect();
    // req: 3.1/V31-4.3-2
    assert_eq!(order, (0..n).collect::<Vec<_>>());
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

pub fn acknowledgements_follow_the_order_of_the_publishes() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut c = session(&broker);
    let ids: Vec<u16> = (100..110).collect();
    for (i, id) in ids.iter().enumerate() {
        let mut p = Publish::new(&t, b"a");
        p.qos = if i % 2 == 0 { 1 } else { 2 };
        p.packet_id = Some(*id);
        c.send(&Packet::Publish(p)).unwrap();
    }
    let mut pubacks = Vec::new();
    let mut pubrecs = Vec::new();
    for _ in &ids {
        match c.expect_packet(default_timeout()).unwrap() {
            Packet::PubAck(a) => pubacks.push(a.packet_id),
            Packet::PubRec(a) => pubrecs.push(a.packet_id),
            other => panic!("expected PUBACK/PUBREC, got {other:?}"),
        }
    }
    // req: 3.1/V31-4.3-1
    assert_eq!(pubacks, ids.iter().copied().step_by(2).collect::<Vec<_>>());
    assert_eq!(
        pubrecs,
        ids.iter().copied().skip(1).step_by(2).collect::<Vec<_>>()
    );
    for id in &pubrecs {
        c.send(&Packet::PubRel(Ack::new(*id))).unwrap();
    }
    // The order of PUBCOMPs is not constrained; each PUBREL gets one.
    let mut comps = HashSet::new();
    for _ in &pubrecs {
        match c.expect_packet(default_timeout()).unwrap() {
            Packet::PubComp(a) => assert!(comps.insert(a.packet_id), "second PUBCOMP {a:?}"),
            other => panic!("expected PUBCOMP, got {other:?}"),
        }
    }
    assert_eq!(comps, pubrecs.iter().copied().collect::<HashSet<_>>());
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
        // req: 3.1/V31-2.4-2
        // req: 3.1/V31-2.4-3
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
        // With a shared broker "conformance/+" can see other tests' traffic:
        // skip it until this case's message arrives (or the wait times out).
        let p = loop {
            // req: 3.1/V31-3.3-5
            let p = s.expect_publish(default_timeout()).unwrap();
            if p.topic == t {
                break p;
            }
        };
        assert_eq!(p.payload, b"all");
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
    // req: 3.1/V31-3.3-3
    assert_eq!(p.payload, b"");
}

pub fn message_ids_appear_exactly_where_required() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    // QoS 0: no Message ID in either direction. The payload starts with
    // bytes that look like one, so an inserted or stripped ID shows.
    let payload = [0x00, 0x2A, b'q', b'0'];
    publisher.publish(&t, &payload, 0, false).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 3.1/V31-2.4-1
    assert_eq!((p.qos, p.payload.as_slice()), (0, &payload[..]), "{p:?}");
    // PUBACK, PUBREC and PUBCOMP echo the client's Message ID (the decoder
    // also requires each to be exactly the 2-byte ID).
    for (qos, id) in [(1u8, 0x4242u16), (2, 0x4343)] {
        let mut m = Publish::new(&t, b"id");
        m.qos = qos;
        m.packet_id = Some(id);
        // req: 3.1/V31-2.4-1
        let ack = publisher.publish_packet(m).unwrap().unwrap();
        assert_eq!(ack.packet_id, id);
    }
    // Deliveries at QoS 1 and 2 carry a non-zero Message ID, and the
    // server's PUBREL repeats the PUBLISH's.
    let q1 = sub.expect_publish(default_timeout()).unwrap();
    // req: 3.1/V31-2.4-1
    // req: 3.1/V31-2.4-3
    assert!(
        q1.qos == 1 && q1.packet_id.is_some_and(|i| i != 0),
        "{q1:?}"
    );
    let q2 = sub.expect_publish_unacked(default_timeout()).unwrap();
    let id2 = q2.packet_id.unwrap();
    // req: 3.1/V31-2.4-3
    assert!(q2.qos == 2 && id2 != 0, "{q2:?}");
    sub.send(&Packet::PubRec(Ack::new(id2))).unwrap();
    // req: 3.1/V31-2.4-1
    match sub.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(a) => assert_eq!(a.packet_id, id2),
        other => panic!("expected PUBREL, got {other:?}"),
    }
    sub.send(&Packet::PubComp(Ack::new(id2))).unwrap();
    // SUBACK and UNSUBACK echo the request's Message ID.
    let id = sub.next_packet_id().wrapping_add(1);
    let ack = sub.unsubscribe(&t).unwrap();
    // req: 3.1/V31-2.4-1
    assert_eq!(ack.packet_id, id);
}

/// Sends a QoS 2 PUBLISH, a second copy of it (with `dup` as given) before
/// releasing it, then PUBREL, and returns how many copies reached the
/// subscriber before and after the PUBREL.
fn qos2_with_repeat(dup: bool) -> Option<(usize, usize)> {
    let broker = crate::Broker::for_test()?;
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    let mut p = Publish::new(&t, b"once");
    p.qos = 2;
    p.packet_id = Some(31);
    let rec = |c: &mut crate::Client| match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRec(a) => assert_eq!(a.packet_id, 31),
        other => panic!("expected PUBREC, got {other:?}"),
    };
    publisher.send(&Packet::Publish(p.clone())).unwrap();
    rec(&mut publisher);
    // Forwarding on PUBLISH (method A) or on PUBREL (method B) are both
    // allowed: whatever arrives before the PUBREL is counted separately.
    let before = sub.collect_publishes(quiet_period()).unwrap().len();
    p.dup = dup;
    publisher.send(&Packet::Publish(p)).unwrap();
    rec(&mut publisher);
    publisher.send(&Packet::PubRel(Ack::new(31))).unwrap();
    match publisher.expect_packet(default_timeout()).unwrap() {
        Packet::PubComp(a) => assert_eq!(a.packet_id, 31),
        other => panic!("expected PUBCOMP, got {other:?}"),
    }
    let after = sub.collect_publishes(quiet_period()).unwrap().len();
    Some((before, after))
}

pub fn qos2_onward_delivery_is_exactly_once_with_either_method() {
    let Some((before, after)) = qos2_with_repeat(true) else {
        crate::outcome::skipped(crate::NO_BROKER);
        return;
    };
    crate::outcome::note(if before == 1 {
        "QoS 2 forwarded on PUBLISH (method A)"
    } else {
        "QoS 2 forwarded on PUBREL (method B)"
    });
    // req: 3.1/V31-3.3-6
    // req: 3.1/V31-4.1-4
    assert_eq!(before + after, 1, "before PUBREL {before}, after {after}");
}

pub fn qos2_repeat_without_dup_is_recognised_by_its_message_id() {
    let Some((before, after)) = qos2_with_repeat(false) else {
        crate::outcome::skipped(crate::NO_BROKER);
        return;
    };
    // DUP is only a hint: the repeated Message ID alone identifies the copy.
    // req: 3.1/V31-2.1-4
    assert_eq!(before + after, 1, "before PUBREL {before}, after {after}");
}

pub fn unauthorized_publish_and_subscribe_are_acknowledged_normally() {
    let broker = broker_or_skip!();
    // A probe for a topic the broker reserves: `$SYS/...` is closed to
    // clients by many implementations.
    let t = format!("$SYS/conformance/{}", unique_id("acl"));
    let mut sub = session(&broker);
    let id = sub.next_packet_id();
    sub.send(&Packet::Subscribe(crate::Subscribe {
        packet_id: id,
        properties: Vec::new(),
        filters: vec![(t.clone(), 1)],
    }))
    .unwrap();
    // 3.1 has no failure code: even a refused subscription gets a SUBACK
    // granting a QoS, and the connection stays open.
    let suback = match sub.next_event(default_timeout()).unwrap() {
        Event::Packet(Packet::SubAck(s)) => s,
        other => panic!("SUBSCRIBE to {t}: expected SUBACK, got {other:?}"),
    };
    assert!(
        suback.codes.len() == 1 && suback.codes[0] <= 2,
        "{suback:?}"
    );
    let mut publisher = session(&broker);
    let mut p = Publish::new(&t, b"acl");
    p.qos = 1;
    p.packet_id = Some(0x0A0A);
    publisher.send(&Packet::Publish(p)).unwrap();
    let acked = match publisher.next_event(default_timeout()).unwrap() {
        Event::Packet(Packet::PubAck(a)) => a.packet_id == 0x0A0A,
        _ => false,
    };
    let delivered = sub
        .collect_publishes(quiet_period())
        .is_ok_and(|got| got.iter().any(|m| m.topic == t));
    if delivered {
        crate::outcome::skipped(format!(
            "broker authorizes clients on {t}; no unauthorized topic to probe"
        ));
        return;
    }
    // Something was not authorized (the PUBLISH or the SUBSCRIBE), yet both
    // were acknowledged normally and neither connection was closed.
    // req: 3.1/V31-3.3-7
    assert!(acked, "PUBLISH to {t} was not acknowledged with PUBACK");
    // req: 3.1/V31-3.3-7
    publisher.ping().unwrap();
    // req: 3.1/V31-3.8-7
    sub.ping().unwrap();
}
