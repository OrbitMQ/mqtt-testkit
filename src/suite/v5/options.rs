// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Subscription options (No Local, Retain As Published, Retain Handling) and
//! Subscription Identifiers (MQTT 5.0).

use crate::{PropValue, Property, Publish, broker_or_skip, default_timeout, prop, quiet_period};
use std::collections::BTreeSet;
use std::time::Duration;

use super::{connack_u32, props_of, session, session_with_ack, topic};

const NO_LOCAL: u8 = 0x04;
const RETAIN_AS_PUBLISHED: u8 = 0x08;
const RETAIN_HANDLING_1: u8 = 0x10;
const RETAIN_HANDLING_2: u8 = 0x20;

fn subscription_identifier(id: u32) -> Vec<Property> {
    vec![Property::new(
        prop::SUBSCRIPTION_IDENTIFIER,
        PropValue::VarInt(id),
    )]
}

/// Subscription Identifiers carried by `p`, sorted.
fn ids_of(p: &Publish) -> BTreeSet<u32> {
    props_of(&p.properties, prop::SUBSCRIPTION_IDENTIFIER)
        .into_iter()
        .map(|v| match v {
            PropValue::VarInt(i) => i,
            other => panic!("Subscription Identifier of type {other:?}"),
        })
        .collect()
}

pub fn no_local_suppresses_own_messages_only() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut c = session(&broker);
    c.subscribe(&t, 1 | NO_LOCAL).unwrap();
    c.publish(&t, b"mine", 1, false).unwrap();
    // req: 5.0/MQTT-3.8.3-3
    c.expect_no_packet(quiet_period()).unwrap();
    let mut other = session(&broker);
    other.publish(&t, b"theirs", 1, false).unwrap();
    // req: 5.0/MQTT-3.8.3-3
    assert_eq!(
        c.expect_publish(default_timeout()).unwrap().payload,
        b"theirs"
    );
}

pub fn without_no_local_own_messages_are_delivered() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut c = session(&broker);
    c.subscribe(&t, 1).unwrap();
    c.publish(&t, b"echo", 1, false).unwrap();
    // req: 5.0/MQTT-4.5.0-1
    assert_eq!(
        c.expect_publish(default_timeout()).unwrap().payload,
        b"echo"
    );
}

pub fn retain_as_published_0_clears_retain_on_live_delivery() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"r", 0, true).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.3.1-12
    assert!(!p.retain, "{p:?}");
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn retain_as_published_1_keeps_the_retain_flag() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, RETAIN_AS_PUBLISHED).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"kept", 0, true).unwrap();
    publisher.publish(&t, b"plain", 0, false).unwrap();
    let got = sub.expect_publishes(2, default_timeout()).unwrap();
    // req: 5.0/MQTT-3.3.1-13
    assert!(got[0].retain, "{:?}", got[0]);
    // req: 5.0/MQTT-3.3.1-13
    assert!(!got[1].retain, "{:?}", got[1]);
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn retain_handling_0_sends_retained_messages_on_every_subscribe() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"rh0", 1, true).unwrap();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.3.1-9
    assert_eq!(p.payload, b"rh0");
    assert!(
        p.retain,
        "retained message sent on subscribe without RETAIN"
    );
    sub.subscribe(&t, 0).unwrap();
    // req: 5.0/MQTT-3.3.1-9
    // req: 5.0/MQTT-3.8.4-4
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().payload,
        b"rh0"
    );
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn retain_handling_1_sends_retained_messages_only_for_new_subscriptions() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"rh1", 1, true).unwrap();
    let mut sub = session(&broker);
    sub.subscribe(&t, RETAIN_HANDLING_1).unwrap();
    // req: 5.0/MQTT-3.3.1-10
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().payload,
        b"rh1"
    );
    sub.subscribe(&t, RETAIN_HANDLING_1).unwrap();
    // req: 5.0/MQTT-3.3.1-10
    sub.expect_no_packet(quiet_period()).unwrap();
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn retain_handling_2_never_sends_retained_messages() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"rh2", 1, true).unwrap();
    let mut sub = session(&broker);
    sub.subscribe(&t, RETAIN_HANDLING_2).unwrap();
    // req: 5.0/MQTT-3.3.1-11
    sub.expect_no_packet(quiet_period()).unwrap();
    // Live messages still flow.
    publisher.publish(&t, b"live", 0, false).unwrap();
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().payload,
        b"live"
    );
    publisher.publish(&t, b"", 0, true).unwrap();
}

/// Strict: Retain Handling 3 is a client-side Protocol Error; closing relies on detection (MQTT-4.13.1-1).
pub fn retain_handling_3_is_a_protocol_error() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    let packet_id = c.next_packet_id();
    c.send(&crate::Packet::Subscribe(crate::Subscribe {
        packet_id,
        properties: Vec::new(),
        filters: vec![(topic(), 0x30)],
    }))
    .unwrap();
    // Retain Handling 3 is a Protocol Error (3.8.3.1), which closes the
    // connection.
    // req: 5.0/MQTT-4.13.1-1
    c.expect_error_close(default_timeout()).unwrap();
}

/// Skips (returns false) when the broker announces no Subscription
/// Identifier support.
fn subscription_ids_available(broker: &crate::Broker) -> bool {
    let (_, ack) = session_with_ack(broker);
    let available = connack_u32(&ack, prop::SUBSCRIPTION_IDENTIFIER_AVAILABLE, 1) == 1;
    if !available {
        crate::outcome::skipped("Subscription Identifier Available = 0");
    }
    available
}

pub fn subscription_identifier_is_returned_with_matching_messages() {
    let broker = broker_or_skip!();
    if !subscription_ids_available(&broker) {
        return;
    }
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe_many(&[(&t, 1)], subscription_identifier(42))
        .unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"id", 1, false).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.3.4-3
    assert_eq!(ids_of(&p), BTreeSet::from([42]), "{p:?}");
}

pub fn overlapping_subscriptions_return_every_subscription_identifier() {
    let broker = broker_or_skip!();
    if !subscription_ids_available(&broker) {
        return;
    }
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe_many(&[(&format!("{t}/+"), 1)], subscription_identifier(7))
        .unwrap();
    sub.subscribe_many(&[(&format!("{t}/#"), 1)], subscription_identifier(300))
        .unwrap();
    sub.subscribe(&format!("{t}/x/#"), 1).unwrap();
    let mut publisher = session(&broker);
    publisher
        .publish(&format!("{t}/x"), b"o", 1, false)
        .unwrap();
    let got = sub.collect_publishes(quiet_period()).unwrap();
    assert_ne!(got, []);
    let all: BTreeSet<u32> = got.iter().flat_map(ids_of).collect();
    // req: 5.0/MQTT-3.3.4-3
    assert_eq!(all, BTreeSet::from([7, 300]), "{got:?}");
    if got.len() == 1 {
        // One copy: it carries every identifier.
        // req: 5.0/MQTT-3.3.4-4
        assert_eq!(ids_of(&got[0]), BTreeSet::from([7, 300]));
    } else {
        // One copy per subscription: each carries at most its own id.
        // req: 5.0/MQTT-3.3.4-5
        assert!(got.iter().all(|p| ids_of(p).len() <= 1), "{got:?}");
    }
}

pub fn subscription_identifier_is_replaced_on_resubscribe() {
    let broker = broker_or_skip!();
    if !subscription_ids_available(&broker) {
        return;
    }
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe_many(&[(&t, 0)], subscription_identifier(5))
        .unwrap();
    sub.subscribe_many(&[(&t, 0)], subscription_identifier(6))
        .unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"6", 0, false).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.8.4-3
    // req: 5.0/MQTT-3.3.4-3
    assert_eq!(ids_of(&p), BTreeSet::from([6]), "{p:?}");
}

/// Strict: Subscription Identifier 0 is a client-side Protocol Error; closing relies on detection (MQTT-4.13.1-1).
pub fn subscription_identifier_zero_is_a_protocol_error() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    let packet_id = c.next_packet_id();
    c.send(&crate::Packet::Subscribe(crate::Subscribe {
        packet_id,
        properties: subscription_identifier(0),
        filters: vec![(topic(), 0)],
    }))
    .unwrap();
    // A Subscription Identifier of 0 is a Protocol Error (3.8.2.1.2).
    // req: 5.0/MQTT-4.13.1-1
    c.expect_error_close(default_timeout()).unwrap();
}

/// Re-subscribing to an identical filter replaces the subscription without
/// losing messages queued or in flight for it (MQTT-3.8.4-4).
pub fn replacing_a_subscription_loses_no_message() {
    let broker = broker_or_skip!();
    let t = topic();
    // Receive Maximum 1 keeps the later messages queued at the server.
    let (mut sub, _) = broker
        .connect_ok(
            &crate::Connect::new(super::V, &crate::unique_id("rsl"))
                .property(prop::RECEIVE_MAXIMUM, PropValue::TwoByte(1)),
        )
        .unwrap();
    sub.subscribe_needing(&t, 1);
    let mut publisher = session(&broker);
    let n = 5u8;
    for i in 0..n {
        publisher.publish(&t, &[i], 1, false).unwrap();
    }
    let first = sub.expect_publish_unacked(default_timeout()).unwrap();
    // Same filter again: the subscription is replaced while messages wait.
    sub.subscribe_needing(&t, 1);
    sub.send(&crate::Packet::PubAck(crate::Ack::new(
        first.packet_id.unwrap(),
    )))
    .unwrap();
    let mut got = vec![first.payload[0]];
    let mut distinct: BTreeSet<u8> = got.iter().copied().collect();
    // No deadline for the rest (a server may release them from a timer);
    // they must arrive, each acknowledged as it comes.
    while distinct.len() < usize::from(n) {
        match sub.expect_publish(Duration::from_secs(15)) {
            Ok(p) => {
                got.push(p.payload[0]);
                distinct.insert(p.payload[0]);
            }
            Err(crate::HarnessError::Timeout(_)) => break,
            Err(e) => panic!("{e}"),
        }
    }
    // req: 5.0/MQTT-3.8.4-4
    assert_eq!(distinct, (0..n).collect(), "after re-subscribing: {got:?}");
    if got.len() > usize::from(n) {
        crate::outcome::note(format!("some messages arrived twice: {got:?}"));
    }
}
