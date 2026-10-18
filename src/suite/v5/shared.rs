// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Shared subscriptions (MQTT 5.0).

use std::collections::HashMap;

use std::time::{Duration, Instant};

use crate::{
    Ack, Broker, Client, Event, Packet, Subscribe, broker_or_skip, default_timeout, prop,
    quiet_period, unique_id,
};

use super::{
    Rejection, connack_u32, forget, persistent, session, session_with_ack, subscribe_rejected,
    topic,
};

/// Whether the broker supports shared subscriptions (prints a skip note if
/// not). When it does not, a shared SUBSCRIBE must fail with 0x9E.
fn shared_available(broker: &Broker) -> bool {
    let (mut c, ack) = session_with_ack(broker);
    if connack_u32(&ack, prop::SHARED_SUBSCRIPTION_AVAILABLE, 1) == 1 {
        return true;
    }
    let r = subscribe_rejected(&mut c, &format!("$share/g/{}", topic()), 0);
    assert!(
        matches!(
            r,
            Rejection::SubAck(0x9E) | Rejection::Closed(None | Some(0x9E))
        ),
        "{r:?}"
    );
    crate::outcome::skipped("Shared Subscription Available = 0");
    false
}

fn group() -> String {
    unique_id("g")
}

/// Publishes 20 QoS 1 messages to a two-member shared subscription and counts
/// how often each payload arrives; `None` (after a skip) without shared
/// subscriptions.
fn shared_deliveries(broker: &crate::Broker) -> Option<(HashMap<u8, usize>, u8)> {
    if !shared_available(broker) {
        return None;
    }
    let t = topic();
    let filter = format!("$share/{}/{t}", group());
    let mut members: Vec<Client> = (0..2).map(|_| session(broker)).collect();
    for m in &mut members {
        let ack = m.subscribe(&filter, 1).unwrap();
        assert!(ack.codes[0] <= 1, "{ack:?}");
    }
    let mut publisher = session(broker);
    let n = 20u8;
    for i in 0..n {
        publisher.publish(&t, &[i], 1, false).unwrap();
    }
    let mut seen: HashMap<u8, usize> = HashMap::new();
    for m in &mut members {
        for p in m.collect_publishes(quiet_period()).unwrap() {
            assert_eq!(p.topic, t);
            *seen.entry(p.payload[0]).or_default() += 1;
        }
    }
    Some((seen, n))
}

pub fn shared_subscription_loses_no_message() {
    let broker = broker_or_skip!();
    let Some((seen, n)) = shared_deliveries(&broker) else {
        return;
    };
    // The shared subscription is a matching subscription (4.5.0-1).
    // req: 5.0/MQTT-4.5.0-1
    assert_eq!(seen.len(), usize::from(n), "lost messages: {seen:?}");
}

/// Section 4.8.2 sends each message to only one session of the group. That text
/// is not a numbered normative statement, so this case cites none.
pub fn each_message_goes_to_exactly_one_group_member() {
    let broker = broker_or_skip!();
    let Some((seen, _)) = shared_deliveries(&broker) else {
        return;
    };
    assert!(seen.values().all(|c| *c == 1), "duplicates: {seen:?}");
}

pub fn shared_and_non_shared_subscribers_each_get_a_copy() {
    let broker = broker_or_skip!();
    if !shared_available(&broker) {
        return;
    }
    let t = topic();
    let mut plain = session(&broker);
    plain.subscribe(&t, 0).unwrap();
    let mut g1 = session(&broker);
    g1.subscribe(&format!("$share/{}/{t}", group()), 0).unwrap();
    let mut g2 = session(&broker);
    g2.subscribe(&format!("$share/{}/{t}", group()), 0).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"fan-out", 0, false).unwrap();
    for c in [&mut plain, &mut g1, &mut g2] {
        // req: 5.0/MQTT-4.5.0-1
        assert_eq!(
            c.expect_publish(default_timeout()).unwrap().payload,
            b"fan-out"
        );
    }
}

pub fn shared_delivery_honours_the_granted_qos() {
    let broker = broker_or_skip!();
    if !shared_available(&broker) {
        return;
    }
    let t = topic();
    let mut m = session(&broker);
    m.subscribe(&format!("$share/{}/{t}", group()), 0).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"q2", 2, false).unwrap();
    let p = m.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-4.8.2-3
    assert_eq!(p.qos, 0, "{p:?}");
}

pub fn shared_wildcard_filter_matches() {
    let broker = broker_or_skip!();
    if !shared_available(&broker) {
        return;
    }
    let t = topic();
    let mut m = session(&broker);
    m.subscribe(&format!("$share/{}/{t}/+/x", group()), 0)
        .unwrap();
    let mut publisher = session(&broker);
    publisher
        .publish(&format!("{t}/a/x"), b"w", 0, false)
        .unwrap();
    let p = m.expect_publish(default_timeout()).unwrap();
    // The PUBLISH carries the real topic, not the $share filter.
    // req: 5.0/MQTT-3.3.2-3
    assert_eq!(p.topic, format!("{t}/a/x"));
}

pub fn unsubscribing_a_shared_subscription_stops_delivery() {
    let broker = broker_or_skip!();
    if !shared_available(&broker) {
        return;
    }
    let t = topic();
    let filter = format!("$share/{}/{t}", group());
    let mut m = session(&broker);
    m.subscribe(&filter, 0).unwrap();
    let ack = m.unsubscribe(&filter).unwrap();
    // req: 5.0/MQTT-3.11.3-2
    assert_eq!(ack.codes, [0x00]);
    let mut publisher = session(&broker);
    publisher.publish(&t, b"x", 0, false).unwrap();
    // req: 5.0/MQTT-3.10.4-2
    m.expect_no_packet(quiet_period()).unwrap();
}

/// Strict: MQTT-4.8.2-1/2 are filter syntax rules for the client; rejecting relies on detection.
pub fn invalid_shared_subscription_filters_are_rejected() {
    let broker = broker_or_skip!();
    if !shared_available(&broker) {
        return;
    }
    let t = topic();
    for bad in [
        "$share/".to_owned(),
        format!("$share//{t}"),
        format!("$share/{}", group()),
        format!("$share/{}/", group()),
        format!("$share/a+b/{t}"),
        format!("$share/a#b/{t}"),
    ] {
        let mut c = session(&broker);
        // req: 5.0/MQTT-4.8.2-1
        // req: 5.0/MQTT-4.8.2-2
        subscribe_rejected(&mut c, &bad, 0);
    }
}

/// Strict: MQTT-3.8.3-4 binds the client; closing relies on detection (MQTT-4.13.1-1).
pub fn no_local_on_a_shared_subscription_is_a_protocol_error() {
    let broker = broker_or_skip!();
    if !shared_available(&broker) {
        return;
    }
    let mut c = session(&broker);
    let packet_id = c.next_packet_id();
    c.send(&Packet::Subscribe(Subscribe {
        packet_id,
        properties: Vec::new(),
        filters: vec![(format!("$share/{}/{}", group(), topic()), 0x04)],
    }))
    .unwrap();
    // req: 5.0/MQTT-3.8.3-4
    c.expect_error_close(default_timeout()).unwrap();
}

pub fn interrupted_qos2_shared_delivery_resumes_on_the_same_client() {
    let broker = broker_or_skip!();
    if !shared_available(&broker) {
        return;
    }
    let t = topic();
    let filter = format!("$share/{}/{t}", group());
    let id = unique_id("sh4");
    let (mut a, _) = persistent(&broker, &id);
    let granted = a.subscribe(&filter, 2).unwrap().codes;
    if granted != [2] {
        // A server may grant less than QoS 2; a QoS 1 delivery is not bound
        // by MQTT-4.8.2-4/5.
        crate::outcome::skipped(format!(
            "shared subscription granted {granted:02x?}, not QoS 2"
        ));
        a.abort();
        forget(&broker, &id);
        return;
    }
    let mut publisher = session(&broker);
    publisher.publish(&t, b"exactly-a", 2, false).unwrap();
    let first = a.expect_publish_unacked(default_timeout()).unwrap();
    // Another member joins, then A's connection drops mid-flow.
    let mut b = session(&broker);
    b.subscribe(&filter, 2).unwrap();
    a.abort();
    // req: 5.0/MQTT-4.8.2-4
    b.expect_no_packet(quiet_period()).unwrap();
    let (mut a, present) = persistent(&broker, &id);
    assert!(present);
    let again = a.expect_publish_unacked(default_timeout()).unwrap();
    // req: 5.0/MQTT-4.8.2-4
    assert_eq!(
        (again.packet_id, again.payload.as_slice()),
        (first.packet_id, &b"exactly-a"[..])
    );
    let pid = again.packet_id.unwrap();
    a.send(&Packet::PubRec(Ack::new(pid))).unwrap();
    match a.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(r) => assert_eq!(r.packet_id, pid),
        other => panic!("expected PUBREL, got {other:?}"),
    }
    a.send(&Packet::PubComp(Ack::new(pid))).unwrap();
    // req: 5.0/MQTT-4.8.2-4
    b.expect_no_packet(quiet_period()).unwrap();
    a.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn interrupted_qos2_shared_delivery_is_not_redirected_when_the_session_ends() {
    let broker = broker_or_skip!();
    if !shared_available(&broker) {
        return;
    }
    let t = topic();
    let filter = format!("$share/{}/{t}", group());
    // Session Expiry Interval 0: the session ends with the connection.
    let mut a = session(&broker);
    let granted = a.subscribe(&filter, 2).unwrap().codes;
    if granted != [2] {
        // A server may grant less than QoS 2; a QoS 1 delivery is not bound
        // by MQTT-4.8.2-4/5.
        crate::outcome::skipped(format!(
            "shared subscription granted {granted:02x?}, not QoS 2"
        ));
        a.abort();
        return;
    }
    let mut publisher = session(&broker);
    publisher.publish(&t, b"lost-with-a", 2, false).unwrap();
    a.expect_publish_unacked(default_timeout()).unwrap();
    let mut b = session(&broker);
    b.subscribe(&filter, 2).unwrap();
    a.abort();
    // req: 5.0/MQTT-4.8.2-5
    b.expect_no_packet(quiet_period()).unwrap();
}

/// Reads from every member in turn, answering each PUBLISH with an error
/// acknowledgement (0x80), until all are quiet for [`quiet_period`].
/// Returns the payloads received.
fn reject_everything(members: &mut [Client]) -> Vec<Vec<u8>> {
    let mut got = Vec::new();
    let mut last = Instant::now();
    while last.elapsed() < quiet_period() {
        for m in members.iter_mut() {
            match m.next_event(Duration::from_millis(20)).unwrap() {
                Event::Packet(Packet::Publish(p)) => {
                    let ack = Ack {
                        packet_id: p.packet_id.unwrap(),
                        reason: Some(0x80),
                        properties: Vec::new(),
                    };
                    let reply = if p.qos == 1 {
                        Packet::PubAck(ack)
                    } else {
                        Packet::PubRec(ack)
                    };
                    m.send(&reply).unwrap();
                    got.push(p.payload);
                    last = Instant::now();
                }
                // Not required after PUBREC 0x80 (MQTT-4.3.3-4), not forbidden.
                Event::Packet(Packet::PubRel(a)) => {
                    crate::outcome::note(format!(
                        "PUBREL {} sent after the client's PUBREC 0x80",
                        a.packet_id
                    ));
                    m.send(&Packet::PubComp(Ack::new(a.packet_id))).unwrap();
                }
                Event::Timeout => {}
                other => panic!("expected a PUBLISH, got {other:?}"),
            }
        }
    }
    got
}

pub fn shared_message_rejected_by_a_member_is_not_offered_to_another() {
    let broker = broker_or_skip!();
    if !shared_available(&broker) {
        return;
    }
    for qos in [1u8, 2] {
        let t = topic();
        let filter = format!("$share/{}/{t}", group());
        let mut members: Vec<Client> = (0..3).map(|_| session(&broker)).collect();
        for m in &mut members {
            m.subscribe(&filter, qos).unwrap();
        }
        let mut publisher = session(&broker);
        let n = 6u8;
        for i in 0..n {
            publisher.publish(&t, &[i], qos, false).unwrap();
        }
        let mut got = reject_everything(&mut members);
        got.sort();
        // Each message reaches one member; after its rejection it is
        // discarded, not sent to another member.
        // req: 5.0/MQTT-4.8.2-6
        assert_eq!(
            got,
            (0..n).map(|i| vec![i]).collect::<Vec<_>>(),
            "QoS {qos}"
        );
    }
}
