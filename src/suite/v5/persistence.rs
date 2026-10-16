// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Persistent sessions, offline queueing and redelivery (MQTT 5.0).

use std::time::Duration;

use crate::{
    Ack, Client, Connect, Packet, PropValue, Property, Publish, broker_or_skip, default_timeout,
    prop, quiet_period, unique_id,
};

use super::{V, forget, persistent, session, topic};

pub fn subscriptions_survive_a_reconnect() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("ps");
    let (mut c, present) = persistent(&broker, &id);
    assert!(!present);
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    let (mut c, present) = persistent(&broker, &id);
    // req: 5.0/MQTT-3.2.2-3
    // req: 5.0/MQTT-3.1.4-4
    assert!(present, "stored session not reported");
    let mut publisher = session(&broker);
    publisher
        .publish(&t, b"still-subscribed", 1, false)
        .unwrap();
    // req: 5.0/MQTT-3.1.2-5
    assert_eq!(
        c.expect_publish(default_timeout()).unwrap().payload,
        b"still-subscribed"
    );
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn qos1_and_qos2_messages_are_queued_while_offline() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pq");
    let (mut c, _) = persistent(&broker, &id);
    let granted = c.subscribe(&t, 2).unwrap().codes[0];
    assert!(granted <= 2, "subscription refused: {granted:#04x}");
    if granted == 0 {
        // QoS 0 messages need not be queued for an offline session.
        crate::outcome::skipped("broker granted QoS 0 for a QoS 2 subscription");
        c.disconnect().unwrap();
        forget(&broker, &id);
        return;
    }
    c.disconnect().unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"one", 1, false).unwrap();
    publisher.publish(&t, b"two", 2, false).unwrap();
    let (mut c, present) = persistent(&broker, &id);
    assert!(present);
    let got = c.expect_publishes(2, default_timeout()).unwrap();
    // req: 5.0/MQTT-3.1.2-5
    // req: 5.0/MQTT-3.1.2-23
    // req: 5.0/MQTT-4.5.0-1
    assert_eq!(got[0].payload, b"one");
    assert_eq!(got[0].qos, granted.min(1));
    assert_eq!(got[1].payload, b"two");
    assert_eq!(got[1].qos, granted.min(2));
    // QoS 0 messages may or may not be queued; not checked.
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn clean_session_discards_the_previous_session() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pc");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    let (mut c, ack) = broker
        .connect_ok(&Connect::new(V, &id).clean(true))
        .unwrap();
    // req: 5.0/MQTT-3.2.2-2
    // req: 5.0/MQTT-3.1.4-4
    assert!(!ack.session_present());
    let mut publisher = session(&broker);
    publisher.publish(&t, b"nobody", 1, false).unwrap();
    // req: 5.0/MQTT-3.1.2-4
    // req: 5.0/MQTT-3.1.4-4
    c.expect_no_packet(quiet_period()).unwrap();
    c.disconnect().unwrap();
}

pub fn session_with_expiry_0_ends_with_the_connection() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pn");
    let (mut c, _) = broker
        .connect_ok(&Connect::new(V, &id).clean(true))
        .unwrap();
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    // Session Expiry Interval absent (0): the session ended with the
    // connection.
    let (mut c, present) = persistent(&broker, &id);
    // req: 5.0/MQTT-4.1.0-2
    // req: 5.0/MQTT-3.2.2-3
    assert!(!present, "a session with expiry 0 was kept");
    let mut publisher = session(&broker);
    publisher.publish(&t, b"nobody", 1, false).unwrap();
    // req: 5.0/MQTT-4.1.0-2
    c.expect_no_packet(quiet_period()).unwrap();
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn unacknowledged_qos1_is_resent_with_dup_and_the_same_identifier() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pr1");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"again", 1, false).unwrap();
    let first = c.expect_publish_unacked(default_timeout()).unwrap();
    assert!(!first.dup);
    c.abort();
    let (mut c, present) = persistent(&broker, &id);
    assert!(present);
    let again = c.expect_publish(default_timeout()).unwrap();
    // Still unacknowledged (no PUBACK was sent), so it is sent again.
    // req: 5.0/MQTT-4.4.0-1
    // req: 5.0/MQTT-4.3.2-3
    assert_eq!(again.packet_id, first.packet_id);
    assert_eq!(again.payload, b"again");
    // req: 5.0/MQTT-3.3.1-1
    assert!(again.dup, "redelivery without DUP: {again:?}");
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn unacknowledged_qos2_publish_is_resent_with_dup() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pr2");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"twice?", 2, false).unwrap();
    let first = c.expect_publish_unacked(default_timeout()).unwrap();
    c.abort();
    let (mut c, _) = persistent(&broker, &id);
    let again = c.expect_publish_unacked(default_timeout()).unwrap();
    // Still unacknowledged (no PUBREC was sent), so it is sent again.
    // req: 5.0/MQTT-4.4.0-1
    // req: 5.0/MQTT-4.3.3-3
    assert_eq!(again.packet_id, first.packet_id);
    assert_eq!(again.qos, 2);
    // req: 5.0/MQTT-3.3.1-1
    assert!(again.dup, "{again:?}");
    let pid = again.packet_id.unwrap();
    c.send(&Packet::PubRec(Ack::new(pid))).unwrap();
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(a) => assert_eq!(a.packet_id, pid),
        other => panic!("expected PUBREL, got {other:?}"),
    }
    c.send(&Packet::PubComp(Ack::new(pid))).unwrap();
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn unacknowledged_pubrel_is_resent_and_publish_is_not() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("prr");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"rel", 2, false).unwrap();
    let p = c.expect_publish_unacked(default_timeout()).unwrap();
    let pid = p.packet_id.unwrap();
    c.send(&Packet::PubRec(Ack::new(pid))).unwrap();
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(a) => assert_eq!(a.packet_id, pid),
        other => panic!("expected PUBREL, got {other:?}"),
    }
    c.abort();
    let (mut c, _) = persistent(&broker, &id);
    // The PUBREL is still unacknowledged (no PUBCOMP), so it is sent again.
    // req: 5.0/MQTT-4.4.0-1
    // req: 5.0/MQTT-4.3.3-5
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(a) => assert_eq!(a.packet_id, pid),
        other => panic!("expected the PUBREL again, got {other:?}"),
    }
    c.send(&Packet::PubComp(Ack::new(pid))).unwrap();
    // req: 5.0/MQTT-4.3.3-6
    c.expect_no_packet(quiet_period()).unwrap();
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn redelivery_keeps_the_original_order() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pro");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    for i in 0..5u8 {
        publisher.publish(&t, &[i], 1, false).unwrap();
    }
    let first: Vec<_> = (0..5)
        .map(|_| c.expect_publish_unacked(default_timeout()).unwrap())
        .collect();
    c.abort();
    let (mut c, _) = persistent(&broker, &id);
    let again = c.expect_publishes(5, default_timeout()).unwrap();
    // req: 5.0/MQTT-4.4.0-1
    assert_eq!(
        again.iter().map(|p| p.packet_id).collect::<Vec<_>>(),
        first.iter().map(|p| p.packet_id).collect::<Vec<_>>()
    );
    assert_eq!(
        again.iter().map(|p| p.payload[0]).collect::<Vec<_>>(),
        [0, 1, 2, 3, 4]
    );
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn inbound_qos2_state_survives_a_reconnect() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 2).unwrap();
    let id = unique_id("pi2");
    let (mut c, _) = persistent(&broker, &id);
    let mut p = Publish::new(&t, b"exactly-once");
    p.qos = 2;
    p.packet_id = Some(21);
    c.send(&Packet::Publish(p.clone())).unwrap();
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRec(a) => assert_eq!(a.packet_id, 21),
        other => panic!("expected PUBREC, got {other:?}"),
    }
    c.abort();
    let (mut c, present) = persistent(&broker, &id);
    assert!(present);
    // The client resends the PUBLISH (DUP) as it never saw the PUBREC
    // completed by PUBCOMP, then releases it.
    p.dup = true;
    c.send(&Packet::Publish(p)).unwrap();
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRec(a) => assert_eq!(a.packet_id, 21),
        other => panic!("expected PUBREC, got {other:?}"),
    }
    c.send(&Packet::PubRel(Ack::new(21))).unwrap();
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubComp(a) => assert_eq!(a.packet_id, 21),
        other => panic!("expected PUBCOMP, got {other:?}"),
    }
    let got = sub.collect_publishes(quiet_period()).unwrap();
    // req: 5.0/MQTT-4.3.3-10
    // req: 5.0/MQTT-3.1.2-5
    assert_eq!(got.len(), 1, "{got:?}");
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn client_identifier_selects_the_session() {
    let broker = broker_or_skip!();
    let t = topic();
    let (a, b) = (unique_id("pa"), unique_id("pb"));
    let (mut c, _) = persistent(&broker, &a);
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"for-a", 1, false).unwrap();
    // Another client identifier gets none of A's session state.
    let (mut other, present) = persistent(&broker, &b);
    // req: 5.0/MQTT-3.1.3-2
    assert!(!present, "client {b} was given another client's session");
    // req: 5.0/MQTT-3.1.3-2
    other.expect_no_packet(quiet_period()).unwrap();
    other.disconnect().unwrap();
    let (mut c, present) = persistent(&broker, &a);
    // req: 5.0/MQTT-3.1.3-2
    assert!(present);
    // req: 5.0/MQTT-3.1.3-2
    assert_eq!(
        c.expect_publish(default_timeout()).unwrap().payload,
        b"for-a"
    );
    c.disconnect().unwrap();
    forget(&broker, &a);
    forget(&broker, &b);
}

pub fn session_state_is_kept_while_the_connection_is_open() {
    let broker = broker_or_skip!();
    let t = topic();
    // A 1 s Session Expiry Interval only starts counting at disconnection.
    let (mut c, _) = broker
        .connect_ok(
            &Connect::new(V, &unique_id("pk"))
                .property(prop::SESSION_EXPIRY_INTERVAL, PropValue::FourByte(1)),
        )
        .unwrap();
    c.subscribe(&t, 1).unwrap();
    for _ in 0..5 {
        std::thread::sleep(Duration::from_millis(500));
        c.ping().unwrap();
    }
    let mut publisher = session(&broker);
    publisher.publish(&t, b"kept", 1, false).unwrap();
    // req: 5.0/MQTT-4.1.0-1
    assert_eq!(
        c.expect_publish(default_timeout()).unwrap().payload,
        b"kept"
    );
}

pub fn unsubscribe_completes_deliveries_already_started() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pu");
    let (mut c, _) = persistent(&broker, &id);
    let (t1, t2) = (format!("{t}/1"), format!("{t}/2"));
    c.subscribe(&t1, 1).unwrap();
    c.subscribe(&t2, 2).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t1, b"one", 1, false).unwrap();
    publisher.publish(&t2, b"two", 2, false).unwrap();
    let mut started = [
        c.expect_publish_unacked(default_timeout()).unwrap(),
        c.expect_publish_unacked(default_timeout()).unwrap(),
    ];
    started.sort_by_key(|p| p.qos);
    let [q1, q2] = started;
    assert_eq!((q1.qos, q2.qos), (1, 2));
    c.unsubscribe_many(&[&t1, &t2]).unwrap();
    // QoS 2: the flow goes on after the UNSUBACK.
    let id2 = q2.packet_id.unwrap();
    c.send(&Packet::PubRec(Ack::new(id2))).unwrap();
    match c.expect_packet(default_timeout()).unwrap() {
        // req: 5.0/MQTT-3.10.4-3
        Packet::PubRel(a) => assert_eq!(a.packet_id, id2),
        other => panic!("expected PUBREL, got {other:?}"),
    }
    c.send(&Packet::PubComp(Ack::new(id2))).unwrap();
    // QoS 1: still unacknowledged, so it is resent on reconnection.
    c.abort();
    let (mut c, present) = persistent(&broker, &id);
    assert!(present);
    let again = c.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.10.4-3
    assert_eq!(
        (again.packet_id, again.payload.as_slice()),
        (q1.packet_id, &b"one"[..])
    );
    // Nothing else: the subscriptions are gone.
    c.expect_no_packet(quiet_period()).unwrap();
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn message_expiry_does_not_apply_once_a_qos2_publish_is_sent() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pe");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    let mut p = Publish::new(&t, b"expiring");
    p.qos = 2;
    p.properties = vec![Property::new(
        prop::MESSAGE_EXPIRY_INTERVAL,
        PropValue::FourByte(1),
    )];
    publisher.publish_packet(p).unwrap();
    let first = c.expect_publish_unacked(default_timeout()).unwrap();
    c.abort();
    std::thread::sleep(Duration::from_millis(2_100));
    let (mut c, _) = persistent(&broker, &id);
    let again = c.expect_publish_unacked(default_timeout()).unwrap();
    // The expiry has passed, but the PUBLISH was already sent.
    // req: 5.0/MQTT-4.3.3-7
    assert_eq!(
        (again.packet_id, again.payload.as_slice()),
        (first.packet_id, &b"expiring"[..])
    );
    let pid = again.packet_id.unwrap();
    c.send(&Packet::PubRec(Ack::new(pid))).unwrap();
    match c.expect_packet(default_timeout()).unwrap() {
        // req: 5.0/MQTT-4.3.3-7
        Packet::PubRel(a) => assert_eq!(a.packet_id, pid),
        other => panic!("expected PUBREL, got {other:?}"),
    }
    c.send(&Packet::PubComp(Ack::new(pid))).unwrap();
    c.disconnect().unwrap();
    forget(&broker, &id);
}

/// Answers `p` with a PUBACK (QoS 1) or PUBREC (QoS 2) carrying `reason`.
fn ack_with(c: &mut Client, p: &Publish, reason: u8) {
    let ack = Ack {
        packet_id: p.packet_id.unwrap(),
        reason: Some(reason),
        properties: Vec::new(),
    };
    let packet = if p.qos == 1 {
        Packet::PubAck(ack)
    } else {
        Packet::PubRec(ack)
    };
    c.send(&packet).unwrap();
}

pub fn publish_rejected_with_an_error_ack_is_not_resent() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("px");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 2).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"reject-1", 1, false).unwrap();
    publisher.publish(&t, b"reject-2", 2, false).unwrap();
    publisher.publish(&t, b"pending", 1, false).unwrap();
    let got: Vec<Publish> = (0..3)
        .map(|_| c.expect_publish_unacked(default_timeout()).unwrap())
        .collect();
    for p in &got {
        if p.payload != b"pending" {
            ack_with(&mut c, p, 0x80);
        }
    }
    // Let the broker process the acknowledgements before the drop.
    c.ping().unwrap();
    c.abort();
    let (mut c, present) = persistent(&broker, &id);
    assert!(present);
    let again: Vec<Vec<u8>> = c
        .collect_publishes(quiet_period())
        .unwrap()
        .into_iter()
        .map(|p| p.payload)
        .collect();
    // Only the unacknowledged message comes back (proving redelivery
    // happens); the two answered with 0x80 do not.
    // req: 5.0/MQTT-4.4.0-2
    assert_eq!(again, [b"pending".to_vec()]);
    c.disconnect().unwrap();
    forget(&broker, &id);
}
