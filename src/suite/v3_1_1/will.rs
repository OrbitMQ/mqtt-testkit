// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Will messages beyond the basics in `lifecycle.rs` (MQTT 3.1.1).

use std::time::Duration;

use crate::{Connect, Will, broker_or_skip, default_timeout, frame, quiet_period, unique_id};

use super::{V, session, topic};

pub fn will_is_published_at_its_qos() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    let granted = sub.subscribe(&t, 2).unwrap().codes[0];
    let mut will = Will::new(&t, b"q1-will");
    will.qos = 1;
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wq")).will(will))
        .unwrap();
    c.abort();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 3.1.1/MQTT-3.8.4-6
    assert_eq!(p.qos, granted.min(1), "{p:?}");
    assert_eq!(p.payload, b"q1-will");
}

pub fn will_without_retain_is_not_retained() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wnr")).will(Will::new(&t, b"plain")))
        .unwrap();
    c.abort();
    let p = sub.expect_publish(default_timeout()).unwrap();
    assert_eq!(p.payload, b"plain");
    // A live subscriber gets RETAIN 0 even for a retained will (MQTT-3.3.1-9);
    // the late subscriber below is what checks MQTT-3.1.2-16.
    // req: 3.1.1/MQTT-3.3.1-9
    assert!(!p.retain);
    let mut late = session(&broker);
    late.subscribe(&t, 0).unwrap();
    // req: 3.1.1/MQTT-3.1.2-16
    late.expect_no_packet(quiet_period()).unwrap();
}

pub fn will_is_published_after_keep_alive_expiry() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (c, _) = broker
        .connect_ok(
            &Connect::new(V, &unique_id("wka"))
                .keep_alive(1)
                .will(Will::new(&t, b"timed-out")),
        )
        .unwrap();
    // Stay silent: the broker must close after 1.5 s and publish the will.
    let p = sub
        .expect_publish(Duration::from_millis(1_500) + default_timeout())
        .unwrap();
    // req: 3.1.1/MQTT-3.1.2-8
    assert_eq!(p.payload, b"timed-out");
    drop(c);
}

pub fn will_is_published_when_the_server_closes_for_a_protocol_violation() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (mut c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wpv")).will(Will::new(&t, b"violated")))
        .unwrap();
    // PUBLISH with QoS 3: the server must close the connection.
    c.send_raw(&frame(0x36, &[0, 1, b'a', 0, 1])).unwrap();
    let _ = c.expect_closed(default_timeout());
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 3.1.1/MQTT-3.1.2-8
    assert_eq!(p.payload, b"violated");
}

pub fn will_is_published_only_once() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let id = unique_id("w1");
    let (c, _) = broker
        .connect_ok(
            &Connect::new(V, &id)
                .clean(false)
                .will(Will::new(&t, b"once")),
        )
        .unwrap();
    c.abort();
    // Exactly one copy: once published, the will is gone from the session.
    // The spec sets no deadline for the first copy.
    let first = sub.expect_publish(default_timeout()).unwrap();
    let more = sub.collect_publishes(quiet_period()).unwrap();
    // req: 3.1.1/MQTT-3.1.2-10
    assert_eq!(
        std::iter::once(&first)
            .chain(&more)
            .map(|p| p.payload.as_slice())
            .collect::<Vec<_>>(),
        [&b"once"[..]]
    );
    // Reconnect without a will and drop again: the published will was
    // removed from the session, so nothing more is sent.
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &id).clean(false))
        .unwrap();
    c.abort();
    // req: 3.1.1/MQTT-3.1.2-10
    sub.expect_no_packet(quiet_period()).unwrap();
    let (c, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    c.disconnect().unwrap();
}

pub fn no_will_is_published_without_the_will_flag() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let id = unique_id("wnf");
    // The session once had a will, discarded by a normal DISCONNECT.
    let (c, _) = broker
        .connect_ok(
            &Connect::new(V, &id)
                .clean(false)
                .will(Will::new(&t, b"stale")),
        )
        .unwrap();
    c.disconnect().unwrap();
    sub.expect_no_packet(quiet_period()).unwrap();
    // Resumed with Will Flag=0, then dropped: there is no will to publish.
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &id).clean(false))
        .unwrap();
    c.abort();
    // req: 3.1.1/MQTT-3.1.2-12
    sub.expect_no_packet(quiet_period()).unwrap();
    let (c, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    c.disconnect().unwrap();
}

pub fn payload_fields_are_read_in_order() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    // Every optional field present: Client Identifier, Will Topic, Will
    // Message, User Name, Password.
    let connect = Connect::new(V, &unique_id("ord"))
        .will(Will::new(&t, b"ordered-will"))
        .username("conformance")
        .password(b"conformance-pw");
    let (mut c, ack) = broker.connect(&connect).unwrap();
    if ack.code != 0 {
        // A broker that checks credentials may refuse these (0x04 / 0x05).
        let _ = c.expect_closed(default_timeout());
        crate::outcome::skipped(format!(
            "broker refused a CONNECT with user name and password (code {:#04x})",
            ack.code
        ));
        return;
    }
    c.abort();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // The server read the payload fields in order and published the will.
    // req: 3.1.1/MQTT-3.1.3-1
    // req: 3.1.1/MQTT-3.1.2-8
    assert_eq!(
        (p.topic.as_str(), p.payload.as_slice()),
        (t.as_str(), &b"ordered-will"[..])
    );
}
