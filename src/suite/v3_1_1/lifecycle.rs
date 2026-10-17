// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Keep-alive, PINGREQ, DISCONNECT and will handling (MQTT 3.1.1).

use std::time::Duration;

use crate::{Connect, Will, broker_or_skip, default_timeout, quiet_period, unique_id};

use super::{V, session};

fn topic() -> String {
    format!("conformance/{}", unique_id("w"))
}

pub fn every_pingreq_gets_a_pingresp() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    for _ in 0..3 {
        // req: 3.1.1/MQTT-3.12.4-1
        c.ping().unwrap();
    }
}

pub fn keep_alive_expiry_closes_the_connection() {
    let broker = broker_or_skip!();
    let (mut c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("ka")).keep_alive(1))
        .unwrap();
    // req: 3.1.1/MQTT-3.1.2-24
    c.check_keep_alive(Duration::from_secs(1)).unwrap();
}

pub fn will_is_published_when_the_connection_drops() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wd")).will(Will::new(&t, b"gone")))
        .unwrap();
    c.abort();
    // req: 3.1.1/MQTT-3.1.2-8
    let p = sub.expect_publish(default_timeout()).unwrap();
    assert_eq!(
        (p.topic.as_str(), p.payload.as_slice()),
        (t.as_str(), &b"gone"[..])
    );
}

pub fn disconnect_discards_the_will() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wn")).will(Will::new(&t, b"gone")))
        .unwrap();
    c.disconnect().unwrap();
    // req: 3.1.1/MQTT-3.14.4-3
    // req: 3.1.1/MQTT-3.1.2-10
    sub.expect_no_packet(quiet_period()).unwrap();
}

pub fn will_retain_publishes_a_retained_will() {
    let broker = broker_or_skip!();
    let t = topic();
    // A live subscriber sees the will first, so it has been published (and
    // retained) before the late subscriber looks for it.
    let mut live = session(&broker);
    live.subscribe(&t, 0).unwrap();
    let mut will = Will::new(&t, b"retained-will");
    // QoS 1: a server MAY discard a retained QoS 0 message at any time.
    will.qos = 1;
    will.retain = true;
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wr")).will(will))
        .unwrap();
    c.abort();
    assert_eq!(
        live.expect_publish(default_timeout()).unwrap().payload,
        b"retained-will"
    );
    let mut late = session(&broker);
    late.subscribe(&t, 0).unwrap();
    let p = late.expect_publish(default_timeout()).unwrap();
    // req: 3.1.1/MQTT-3.1.2-17
    assert!(p.retain, "will was not retained");
    assert_eq!(p.payload, b"retained-will");
    late.publish(&t, b"", 0, true).unwrap();
}

pub fn a_clean_session_lasts_as_long_as_the_connection() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let mut publisher = session(&broker);
    for round in 0..2u8 {
        publisher.publish(&t, &[round], 1, false).unwrap();
        // req: 3.1.1/MQTT-4.1.0-2
        assert_eq!(
            sub.expect_publish(default_timeout()).unwrap().payload,
            [round]
        );
        std::thread::sleep(quiet_period());
    }
}
