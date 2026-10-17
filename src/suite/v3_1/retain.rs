// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Retained messages (MQTT 3.1).

use crate::{broker_or_skip, default_timeout, quiet_period};

use super::{session, topic};

// Retained messages that a later subscriber must find are published at
// QoS 1 or 2, so the flow (PUBACK / PUBCOMP) has completed, and the message
// is stored, before that subscriber connects.

pub fn retained_message_is_sent_to_new_subscribers_with_retain_1() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"kept", 1, true).unwrap();
    let mut late = session(&broker);
    late.subscribe_needing(&t, 1);
    let p = late.expect_publish(default_timeout()).unwrap();
    // req: 3.1/V31-2.1-6
    // req: 3.1/V31-2.1-7
    assert_eq!(p.payload, b"kept");
    assert!(p.retain, "{p:?}");
    // The stored QoS (1) is delivered, as the subscription allows it.
    // req: 3.1/V31-2.1-6
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
    // req: 3.1/V31-3.8-3
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
    // req: 3.1/V31-2.1-6
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0].payload, b"new");
    publisher.publish(&t, b"", 0, true).unwrap();
}

/// Informative: deleting the retained message on an empty retained PUBLISH
/// is a MAY in 3.1 (V31-2.1-10), so the outcome is only noted.
pub fn empty_retained_message_deletion_is_noted() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"doomed", 1, true).unwrap();
    publisher.publish(&t, b"", 1, true).unwrap();
    let mut late = session(&broker);
    late.subscribe(&t, 1).unwrap();
    let got = late.collect_publishes(quiet_period()).unwrap();
    crate::outcome::note(if got.is_empty() {
        "an empty retained PUBLISH deleted the retained message".to_owned()
    } else {
        format!("an empty retained PUBLISH did not delete the retained message: {got:?}")
    });
}

pub fn empty_retained_message_is_delivered_to_current_subscribers() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"", 0, true).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 3.1/V31-3.3-3
    assert_eq!(p.payload, b"");
    // req: 3.1/V31-2.1-8
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
    // req: 3.1/V31-2.1-8
    assert!(!p.retain, "{p:?}");
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
    // req: 3.1/V31-2.1-7
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

pub fn retained_message_outlives_the_publishers_session() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    publisher.publish(&t, b"durable", 1, true).unwrap();
    publisher.disconnect().unwrap();
    let mut late = session(&broker);
    late.subscribe(&t, 0).unwrap();
    // req: 3.1/V31-2.1-6
    assert_eq!(
        late.expect_publish(default_timeout()).unwrap().payload,
        b"durable"
    );
    late.publish(&t, b"", 0, true).unwrap();
}
