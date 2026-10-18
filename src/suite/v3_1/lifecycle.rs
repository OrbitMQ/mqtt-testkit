// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Keep-alive, PINGREQ, DISCONNECT and will handling (MQTT 3.1).

use std::time::Duration;

use crate::{
    Connect, Packet, ReasonOnly, Will, broker_or_skip, default_timeout, quiet_period, unique_id,
};

use super::{V, session};

fn topic() -> String {
    format!("conformance/{}", unique_id("w"))
}

pub fn every_pingreq_gets_a_pingresp() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    for _ in 0..3 {
        // req: 3.1/V31-3.12-1
        c.ping().unwrap();
    }
}

pub fn keep_alive_expiry_closes_and_publishes_the_will() {
    let broker = broker_or_skip!();
    let t = topic();
    let own = format!("{t}/own");
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let id = unique_id("ka");
    // CleanSession=0, so the subscription outlives the connection.
    let (mut c, _) = broker
        .connect_ok(
            &Connect::new(V, &id)
                .clean(false)
                .keep_alive(1)
                .will(Will::new(&t, b"expired")),
        )
        .unwrap();
    c.subscribe(&own, 1).unwrap();
    // req: 3.1/V31-2.2-14
    c.check_keep_alive(Duration::from_secs(1)).unwrap();
    // req: 3.1/V31-2.2-8
    // req: 3.1/V31-2.2-14
    let p = sub.expect_publish(default_timeout()).unwrap();
    assert_eq!(p.payload, b"expired");
    // The disconnected client kept its subscription.
    let (mut c, _) = broker
        .connect_ok(&Connect::new(V, &id).clean(false))
        .unwrap();
    sub.publish(&own, b"kept", 1, false).unwrap();
    // req: 3.1/V31-2.2-14
    assert_eq!(
        c.expect_publish(default_timeout()).unwrap().payload,
        b"kept"
    );
    c.disconnect().unwrap();
    let (c, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    c.disconnect().unwrap();
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
    // req: 3.1/V31-2.2-8
    let p = sub.expect_publish(default_timeout()).unwrap();
    assert_eq!(p.topic, t);
    assert_eq!(p.payload, b"gone");
}

pub fn disconnect_discards_the_will_and_server_closes() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (mut c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wn")).will(Will::new(&t, b"gone")))
        .unwrap();
    // Send DISCONNECT but keep our side of the socket open.
    c.send(&Packet::Disconnect(ReasonOnly::default())).unwrap();
    // req: 3.1/V31-3.14-2
    c.expect_closed(default_timeout()).unwrap();
    // req: 3.1/V31-3.14-3
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
    // req: 3.1/V31-2.2-12
    assert!(p.retain, "will was not retained");
    assert_eq!(p.payload, b"retained-will");
    late.publish(&t, b"", 0, true).unwrap();
}

pub fn keep_alive_zero_is_never_timed_out() {
    let broker = broker_or_skip!();
    let (mut c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("ka0")).keep_alive(0))
        .unwrap();
    // A 1 s keep-alive would have expired after 1.5 s; stay silent longer.
    // req: 3.1/V31-2.2-15
    c.expect_no_packet(Duration::from_secs(3).max(quiet_period()))
        .unwrap();
    // req: 3.1/V31-2.2-15
    c.ping().unwrap();
}
