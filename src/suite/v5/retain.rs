// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Retained messages (MQTT 5.0).

use crate::{broker_or_skip, default_timeout, quiet_period};

use super::{session, topic};

pub fn retained_message_is_sent_to_new_subscribers_with_retain_1() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"kept", 1, true).unwrap();
    let mut late = session(&broker);
    late.subscribe(&t, 1).unwrap();
    let p = late.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.3.1-5
    // req: 5.0/MQTT-3.3.1-9
    assert_eq!(p.payload, b"kept");
    // "These messages are sent with the RETAIN flag set to 1" (§3.3.1.3,
    // not a numbered statement).
    assert!(p.retain, "{p:?}");
    // The stored QoS (1) is delivered, as the subscription allows it.
    // req: 5.0/MQTT-3.3.1-5
    assert_eq!(p.qos, 1);
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn retained_message_is_downgraded_to_the_subscription_qos() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"kept", 2, true).unwrap();
    let mut late = session(&broker);
    late.subscribe(&t, 0).unwrap();
    let p = late.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.8.4-8
    assert_eq!((p.qos, p.retain), (0, true), "{p:?}");
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn new_retained_message_replaces_the_old_one() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"old", 1, true).unwrap();
    publisher.publish(&t, b"new", 1, true).unwrap();
    let mut late = session(&broker);
    late.subscribe(&t, 1).unwrap();
    let got = late.collect_publishes(quiet_period()).unwrap();
    // req: 5.0/MQTT-3.3.1-5
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0].payload, b"new");
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn empty_retained_message_deletes_and_is_not_stored() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"doomed", 1, true).unwrap();
    publisher.publish(&t, b"", 1, true).unwrap();
    let mut late = session(&broker);
    late.subscribe(&t, 1).unwrap();
    // req: 5.0/MQTT-3.3.1-6
    // req: 5.0/MQTT-3.3.1-7
    late.expect_no_packet(quiet_period()).unwrap();
}

pub fn empty_retained_message_is_delivered_to_current_subscribers() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"", 0, true).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.3.1-6
    assert_eq!(p.payload, b"");
    // req: 5.0/MQTT-3.3.1-12
    assert!(!p.retain);
}

pub fn live_delivery_of_a_retained_publish_has_retain_0() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"live", 1, true).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.3.1-12
    assert!(!p.retain, "{p:?}");
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn non_retained_publish_leaves_the_retained_message_alone() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"keep-me", 1, true).unwrap();
    publisher.publish(&t, b"transient", 1, false).unwrap();
    let mut late = session(&broker);
    late.subscribe(&t, 1).unwrap();
    let got = late.collect_publishes(quiet_period()).unwrap();
    // req: 5.0/MQTT-3.3.1-8
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0].payload, b"keep-me");
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn wildcard_subscription_receives_every_matching_retained_message() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    for leaf in ["a", "b", "c/d"] {
        publisher
            .publish(&format!("{t}/{leaf}"), leaf.as_bytes(), 1, true)
            .unwrap();
    }
    let mut late = session(&broker);
    late.subscribe(&format!("{t}/#"), 0).unwrap();
    let got = late.collect_publishes(quiet_period()).unwrap();
    let mut topics: Vec<_> = got.iter().map(|p| p.topic.clone()).collect();
    topics.sort();
    // req: 5.0/MQTT-3.3.1-9
    assert_eq!(
        topics,
        [format!("{t}/a"), format!("{t}/b"), format!("{t}/c/d")]
    );
    assert!(got.iter().all(|p| p.retain));
    for leaf in ["a", "b", "c/d"] {
        publisher
            .publish(&format!("{t}/{leaf}"), b"", 0, true)
            .unwrap();
    }
}

pub fn resubscribing_resends_retained_messages() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"again", 1, true).unwrap();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().payload,
        b"again"
    );
    sub.subscribe(&t, 1).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.8.4-4
    assert_eq!(p.payload, b"again");
    assert!(p.retain);
    publisher.publish(&t, b"", 0, true).unwrap();
}

/// True (after recording a skip) when the server supports retained
/// messages, i.e. its CONNACK has no Retain Available = 0.
fn retain_available(broker: &crate::Broker) -> bool {
    let (_, ack) = super::session_with_ack(broker);
    let available = super::connack_u32(&ack, crate::prop::RETAIN_AVAILABLE, 1);
    if available == 1 {
        crate::outcome::skipped("broker supports retained messages (Retain Available = 1)");
        return true;
    }
    false
}

pub fn will_retain_without_retain_available_is_refused() {
    let broker = broker_or_skip!();
    if retain_available(&broker) {
        return;
    }
    let mut will = crate::Will::new(&topic(), b"will");
    will.retain = true;
    let connect = crate::Connect::new(super::V, &crate::unique_id("wrn")).will(will);
    // 0x9A is recommended, any refusal is allowed.
    // req: 5.0/MQTT-3.2.2-13
    super::refused(&broker, &connect);
}

/// Strict: MQTT-3.2.2-14 binds the client; closing relies on detection.
pub fn retained_publish_without_retain_available_is_disconnected_with_0x9a() {
    let broker = broker_or_skip!();
    if retain_available(&broker) {
        return;
    }
    let mut c = session(&broker);
    let mut p = crate::Publish::new(&topic(), b"keep");
    p.retain = true;
    // On purpose without Retain Available: bypass the harness's gate.
    c.send_raw(&crate::Packet::Publish(p).encode(super::V))
        .unwrap();
    // req: 5.0/MQTT-3.2.2-14
    let reason = c.expect_error_close(default_timeout()).unwrap();
    // 0x9A is a SHOULD (§3.2.2.3.5); any error code is allowed and noted.
    if let Some(r) = reason.filter(|r| *r != 0x9A) {
        crate::outcome::note(format!(
            "DISCONNECT {r:#04x}, not 0x9A (Retain not supported)"
        ));
    }
}
