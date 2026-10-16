// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Persistent sessions, offline queueing and redelivery (MQTT 3.1.1).

use crate::{
    Ack, Broker, Connect, Packet, Publish, broker_or_skip, default_timeout, quiet_period, unique_id,
};

use super::{V, persistent, session, topic};

/// Removes the session of `client_id` (CleanSession=1 connect + DISCONNECT).
fn forget(broker: &Broker, client_id: &str) {
    let (c, _) = broker
        .connect_ok(&Connect::new(V, client_id).clean(true))
        .unwrap();
    c.disconnect().unwrap();
}

pub fn subscriptions_survive_a_reconnect() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("ps");
    let (mut c, present) = persistent(&broker, &id);
    assert!(!present);
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    let (mut c, present) = persistent(&broker, &id);
    // req: 3.1.1/MQTT-3.2.2-2
    assert!(present, "stored session not reported");
    let mut publisher = session(&broker);
    publisher
        .publish(&t, b"still-subscribed", 1, false)
        .unwrap();
    // req: 3.1.1/MQTT-3.1.2-4
    // req: 3.1.1/MQTT-3.1.4-3
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
    c.disconnect().unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"one", 1, false).unwrap();
    publisher.publish(&t, b"two", 2, false).unwrap();
    let (mut c, present) = persistent(&broker, &id);
    assert!(present);
    let got = c.expect_publishes(2, default_timeout()).unwrap();
    // req: 3.1.1/MQTT-3.1.2-5
    // req: 3.1.1/MQTT-4.5.0-1
    assert_eq!(got[0].payload, b"one");
    assert_eq!(got[0].qos, granted.min(1));
    assert_eq!(got[1].payload, b"two");
    assert_eq!(got[1].qos, granted.min(2));
    // QoS 0 messages may or may not be queued in 3.1.1; not checked.
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
    // req: 3.1.1/MQTT-3.2.2-1
    assert!(!ack.session_present());
    let mut publisher = session(&broker);
    publisher.publish(&t, b"nobody", 1, false).unwrap();
    // req: 3.1.1/MQTT-3.1.2-6
    // req: 3.1.1/MQTT-3.1.4-3
    c.expect_no_packet(quiet_period()).unwrap();
    c.disconnect().unwrap();
}

pub fn clean_session_state_is_not_reused_later() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pn");
    let (mut c, _) = broker
        .connect_ok(&Connect::new(V, &id).clean(true))
        .unwrap();
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    let (mut c, present) = persistent(&broker, &id);
    // req: 3.1.1/MQTT-3.1.2-6
    // req: 3.1.1/MQTT-3.2.2-3
    assert!(!present, "a CleanSession=1 session was kept");
    let mut publisher = session(&broker);
    publisher.publish(&t, b"nobody", 1, false).unwrap();
    // req: 3.1.1/MQTT-3.1.2-6
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
    // req: 3.1.1/MQTT-4.4.0-1
    // req: 3.1.1/MQTT-2.3.1-4
    assert_eq!(again.packet_id, first.packet_id);
    assert_eq!(again.payload, b"again");
    // req: 3.1.1/MQTT-3.3.1-1
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
    // req: 3.1.1/MQTT-4.4.0-1
    assert_eq!(again.packet_id, first.packet_id);
    assert_eq!(again.qos, 2);
    // req: 3.1.1/MQTT-3.3.1-1
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
    // req: 3.1.1/MQTT-4.4.0-1
    // req: 3.1.1/MQTT-4.3.3-1
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(a) => assert_eq!(a.packet_id, pid),
        other => panic!("expected the PUBREL again, got {other:?}"),
    }
    c.send(&Packet::PubComp(Ack::new(pid))).unwrap();
    // req: 3.1.1/MQTT-4.3.3-1
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
    // req: 3.1.1/MQTT-4.6.0-1
    // req: 3.1.1/MQTT-4.4.0-1
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
    // req: 3.1.1/MQTT-4.3.3-2
    // req: 3.1.1/MQTT-4.1.0-1
    assert_eq!(got.len(), 1, "{got:?}");
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn session_state_is_keyed_by_client_id() {
    let broker = broker_or_skip!();
    let t = topic();
    let (a, b) = (unique_id("pka"), unique_id("pkb"));
    let (mut c, _) = persistent(&broker, &a);
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    // Another ClientId does not see A's session.
    let (mut other, present) = persistent(&broker, &b);
    // req: 3.1.1/MQTT-3.1.3-2
    assert!(!present, "{b} was given the session of {a}");
    let mut publisher = session(&broker);
    publisher.publish(&t, b"for-a", 1, false).unwrap();
    // req: 3.1.1/MQTT-3.1.3-2
    other.expect_no_packet(quiet_period()).unwrap();
    other.disconnect().unwrap();
    // A's ClientId finds its session, with the queued message.
    let (mut c, present) = persistent(&broker, &a);
    // req: 3.1.1/MQTT-3.1.3-2
    assert!(present, "session of {a} not found");
    assert_eq!(
        c.expect_publish(default_timeout()).unwrap().payload,
        b"for-a"
    );
    c.disconnect().unwrap();
    forget(&broker, &a);
    forget(&broker, &b);
}

pub fn unsubscribe_does_not_cancel_an_in_flight_qos1_delivery() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pu1");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"started", 1, false).unwrap();
    let first = c.expect_publish_unacked(default_timeout()).unwrap();
    c.unsubscribe(&t).unwrap();
    // The delivery was started but not acknowledged: the server must still
    // complete it, so it is resent when the session resumes.
    c.abort();
    let (mut c, present) = persistent(&broker, &id);
    assert!(present);
    let again = c.expect_publish(default_timeout()).unwrap();
    // req: 3.1.1/MQTT-3.10.4-3
    assert_eq!(
        (again.packet_id, again.payload.as_slice()),
        (first.packet_id, &b"started"[..]),
        "{again:?}"
    );
    // The subscription itself is gone.
    publisher.publish(&t, b"after", 1, false).unwrap();
    c.expect_no_packet(quiet_period()).unwrap();
    c.disconnect().unwrap();
    forget(&broker, &id);
}
