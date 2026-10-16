// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Persistent sessions, offline queueing and redelivery (MQTT 3.1).

use crate::{
    Ack, Broker, Connect, Packet, Publish, broker_or_skip, default_timeout, quiet_period, unique_id,
};

use super::{V, persistent, session, topic};

// 3.1 has no Session Present flag (the CONNACK flags byte is reserved), so
// session survival is observed through subscriptions and queued messages.

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
    let mut c = persistent(&broker, &id);
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    let mut c = persistent(&broker, &id);
    let mut publisher = session(&broker);
    publisher
        .publish(&t, b"still-subscribed", 1, false)
        .unwrap();
    // req: 3.1/V31-2.2-3
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
    let mut c = persistent(&broker, &id);
    let granted = c.subscribe(&t, 2).unwrap().codes[0];
    c.disconnect().unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"one", 1, false).unwrap();
    publisher.publish(&t, b"two", 2, false).unwrap();
    let mut c = persistent(&broker, &id);
    let got = c.expect_publishes(2, default_timeout()).unwrap();
    // req: 3.1/V31-2.2-3
    // req: 3.1/V31-3.3-5
    // 3.1 ordering is conditional (§4.3), so the two are matched by payload.
    let delivered = |payload: &[u8]| {
        got.iter()
            .find(|p| p.payload == payload)
            .unwrap_or_else(|| panic!("{payload:?} not delivered: {got:?}"))
            .qos
    };
    assert_eq!(delivered(b"one"), granted.min(1));
    assert_eq!(delivered(b"two"), granted.min(2));
    // QoS 0 messages may or may not be queued in 3.1; not checked.
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn clean_session_discards_the_previous_session() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pc");
    let mut c = persistent(&broker, &id);
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    let (mut c, _) = broker
        .connect_ok(&Connect::new(V, &id).clean(true))
        .unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"nobody", 1, false).unwrap();
    // req: 3.1/V31-2.2-4
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
    let mut c = persistent(&broker, &id);
    let mut publisher = session(&broker);
    publisher.publish(&t, b"nobody", 1, false).unwrap();
    // The DISCONNECT of the CleanSession=1 client discarded its subscription.
    // req: 3.1/V31-2.2-4
    // req: 3.1/V31-3.14-1
    c.expect_no_packet(quiet_period()).unwrap();
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn unacknowledged_qos1_is_resent_with_dup_and_the_same_identifier() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pr1");
    let mut c = persistent(&broker, &id);
    c.subscribe_needing(&t, 1);
    let mut publisher = session(&broker);
    publisher.publish(&t, b"again", 1, false).unwrap();
    let first = c.expect_publish_unacked(default_timeout()).unwrap();
    assert!(!first.dup);
    c.abort();
    let mut c = persistent(&broker, &id);
    let again = c.expect_publish(default_timeout()).unwrap();
    // req: 3.1/V31-4.2-3
    assert_eq!(again.packet_id, first.packet_id);
    assert_eq!(again.payload, b"again");
    // req: 3.1/V31-2.1-3
    assert!(again.dup, "redelivery without DUP: {again:?}");
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn unacknowledged_qos2_publish_is_resent_with_dup() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pr2");
    let mut c = persistent(&broker, &id);
    c.subscribe_needing(&t, 2);
    let mut publisher = session(&broker);
    publisher.publish(&t, b"twice?", 2, false).unwrap();
    let first = c.expect_publish_unacked(default_timeout()).unwrap();
    c.abort();
    let mut c = persistent(&broker, &id);
    let again = c.expect_publish_unacked(default_timeout()).unwrap();
    // req: 3.1/V31-4.2-3
    assert_eq!(again.packet_id, first.packet_id);
    assert_eq!(again.qos, 2);
    // req: 3.1/V31-2.1-3
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
    let mut c = persistent(&broker, &id);
    c.subscribe_needing(&t, 2);
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
    let mut c = persistent(&broker, &id);
    // req: 3.1/V31-4.2-3
    // req: 3.1/V31-3.5-1
    // req: 3.1/V31-4.1-3
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(a) => assert_eq!(a.packet_id, pid),
        other => panic!("expected the PUBREL again, got {other:?}"),
    }
    c.send(&Packet::PubComp(Ack::new(pid))).unwrap();
    // req: 3.1/V31-3.5-1
    // req: 3.1/V31-4.1-3
    c.expect_no_packet(quiet_period()).unwrap();
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn redelivery_keeps_the_original_order() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pro");
    let mut c = persistent(&broker, &id);
    c.subscribe_needing(&t, 1);
    let mut publisher = session(&broker);
    for i in 0..5u8 {
        publisher.publish(&t, &[i], 1, false).unwrap();
    }
    // Unacknowledged; a server may keep fewer than 5 in flight, the rest
    // queued behind them.
    let first = c.expect_in_flight(5, default_timeout()).unwrap();
    c.abort();
    let mut c = persistent(&broker, &id);
    let again = c.expect_publishes(5, default_timeout()).unwrap();
    // req: 3.1/V31-4.2-3
    assert_eq!(
        again
            .iter()
            .take(first.len())
            .map(|p| p.packet_id)
            .collect::<Vec<_>>(),
        first.iter().map(|p| p.packet_id).collect::<Vec<_>>()
    );
    // 3.1 ordering is conditional on the implementation offering it (§4.3).
    let order: Vec<u8> = again.iter().map(|p| p.payload[0]).collect();
    if order != [0, 1, 2, 3, 4] {
        crate::outcome::note(format!("redelivered out of publish order: {order:?}"));
    }
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn inbound_qos2_state_survives_a_reconnect() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 2).unwrap();
    let id = unique_id("pi2");
    let mut c = persistent(&broker, &id);
    let mut p = Publish::new(&t, b"exactly-once");
    p.qos = 2;
    p.packet_id = Some(21);
    c.send(&Packet::Publish(p.clone())).unwrap();
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRec(a) => assert_eq!(a.packet_id, 21),
        other => panic!("expected PUBREC, got {other:?}"),
    }
    c.abort();
    let mut c = persistent(&broker, &id);
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
    // req: 3.1/V31-4.1-3
    // req: 3.1/V31-2.2-3
    assert_eq!(got.len(), 1, "{got:?}");
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn qos0_messages_queued_while_offline_are_noted() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pq0");
    let mut c = persistent(&broker, &id);
    c.subscribe(&t, 2).unwrap();
    c.disconnect().unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"q0", 0, false).unwrap();
    publisher.publish(&t, b"q1", 1, false).unwrap();
    let mut c = persistent(&broker, &id);
    let got: Vec<Vec<u8>> = c
        .collect_publishes(quiet_period())
        .unwrap()
        .into_iter()
        .map(|p| p.payload)
        .collect();
    // req: 3.1/V31-2.2-3
    assert!(
        got.contains(&b"q1".to_vec()),
        "queued QoS 1 message lost: {got:?}"
    );
    // "QoS 0 messages are never stored" is descriptive prose in 3.1 (§2.2),
    // not a requirement, so a stored QoS 0 message is only noted.
    if got.contains(&b"q0".to_vec()) {
        crate::outcome::note("QoS 0 message queued for the offline session");
    }
    c.disconnect().unwrap();
    forget(&broker, &id);
}
