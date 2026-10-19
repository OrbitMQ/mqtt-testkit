// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Keep-alive, PINGREQ, DISCONNECT and will handling (MQTT 5.0).

use std::time::Duration;

use crate::{
    Connect, Packet, PropValue, ReasonOnly, Will, broker_or_skip, default_timeout, find_prop, prop,
    quiet_period, unique_id,
};

use super::{V, session};

fn topic() -> String {
    format!("conformance/{}", unique_id("w"))
}

pub fn every_pingreq_gets_a_pingresp() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    for _ in 0..3 {
        // req: 5.0/MQTT-3.12.4-1
        c.ping().unwrap();
    }
}

pub fn keep_alive_expiry_closes_the_connection() {
    let broker = broker_or_skip!();
    let (mut c, ack) = broker
        .connect_ok(&Connect::new(V, &unique_id("ka")).keep_alive(1))
        .unwrap();
    match find_prop(&ack.properties, prop::SERVER_KEEP_ALIVE) {
        None => {
            // The client's own Keep Alive applies.
            // req: 5.0/MQTT-3.1.2-22
            // req: 5.0/MQTT-3.2.2-22
            c.check_keep_alive(Duration::from_secs(1)).unwrap();
        }
        Some(PropValue::TwoByte(k)) if (1..=3).contains(k) => {
            // req: 5.0/MQTT-3.1.2-22
            c.check_keep_alive(Duration::from_secs(u64::from(*k)))
                .unwrap();
        }
        Some(other) => {
            crate::outcome::skipped(format!("broker overrides keep-alive with {other:?}"));
        }
    }
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
    // req: 5.0/MQTT-3.1.2-7
    // req: 5.0/MQTT-3.1.2-8
    let p = sub.expect_publish(default_timeout()).unwrap();
    assert_eq!(p.topic, t);
    assert_eq!(p.payload, b"gone");
}

pub fn normal_disconnect_discards_the_will() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wn")).will(Will::new(&t, b"gone")))
        .unwrap();
    c.disconnect().unwrap();
    // req: 5.0/MQTT-3.14.4-3
    // req: 5.0/MQTT-3.1.2-10
    sub.expect_no_packet(quiet_period()).unwrap();
}

pub fn disconnect_with_will_message_publishes_the_will() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("w4")).will(Will::new(&t, b"asked")))
        .unwrap();
    // The Will is due once the connection is closed (MQTT-3.1.2-8); the
    // sender of a DISCONNECT closes it (MQTT-3.14.4-2).
    c.disconnect_with(ReasonOnly {
        reason: Some(0x04),
        properties: Vec::new(),
    })
    .unwrap();
    // req: 5.0/MQTT-3.1.2-8
    let p = sub.expect_publish(default_timeout()).unwrap();
    assert_eq!(p.payload, b"asked");
}

pub fn will_retain_publishes_a_retained_will() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut will = Will::new(&t, b"retained-will");
    // QoS 1: a server MAY discard a retained QoS 0 message at any time.
    will.qos = 1;
    will.retain = true;
    let mut live = session(&broker);
    live.subscribe(&t, 0).unwrap();
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wr")).will(will))
        .unwrap();
    c.abort();
    // Once a live subscriber has the will, it has been published (and so
    // stored as retained) before the late subscriber looks.
    assert_eq!(
        live.expect_publish(default_timeout()).unwrap().payload,
        b"retained-will"
    );
    let mut late = session(&broker);
    late.subscribe(&t, 0).unwrap();
    let p = late.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.1.2-15
    assert!(p.retain, "will was not retained");
    assert_eq!(p.payload, b"retained-will");
    late.publish(&t, b"", 0, true).unwrap();
}

/// The DISCONNECT Reason Codes defined by the 5.0 table (3.14.2.1).
const DISCONNECT_CODES: [u8; 29] = [
    0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8D, 0x8E, 0x8F, 0x90, 0x93, 0x94, 0x95,
    0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2,
];

/// Requires the connection to close; if the broker sent a DISCONNECT it
/// must be the last packet. Returns whether one was sent.
fn disconnect_is_last(c: &mut crate::Client, what: &str) -> bool {
    // req: 5.0/MQTT-3.14.4-2
    let packets = c
        .expect_closed(default_timeout())
        .unwrap_or_else(|e| panic!("{what}: {e}"));
    let Some(pos) = packets
        .iter()
        .position(|p| matches!(p, Packet::Disconnect(_)))
    else {
        return false;
    };
    if let Packet::Disconnect(d) = &packets[pos] {
        let code = d.reason.unwrap_or(0);
        // req: 5.0/MQTT-3.14.2-1
        assert!(
            DISCONNECT_CODES.contains(&code),
            "{what}: undefined DISCONNECT code {code:#04x}"
        );
    }
    // req: 5.0/MQTT-3.14.4-1
    assert_eq!(
        pos + 1,
        packets.len(),
        "{what}: packets after DISCONNECT: {packets:?}"
    );
    true
}

pub fn server_sends_nothing_after_its_disconnect() {
    let broker = broker_or_skip!();
    let mut seen = false;

    // Session takeover while messages flow to the old connection.
    let t = topic();
    let id = unique_id("dl");
    let (mut old, _) = super::persistent(&broker, &id);
    old.subscribe(&t, 0).unwrap();
    let mut publisher = session(&broker);
    for i in 0..10u8 {
        publisher.publish(&t, &[i], 0, false).unwrap();
    }
    let (new, _) = super::persistent(&broker, &id);
    for i in 10..20u8 {
        publisher.publish(&t, &[i], 0, false).unwrap();
    }
    // The takeover closes the old connection (MQTT-3.1.4-3).
    // req: 5.0/MQTT-3.1.4-3
    seen |= disconnect_is_last(&mut old, "session takeover");
    new.disconnect().unwrap();
    super::forget(&broker, &id);

    // Malformed PUBLISH (QoS 3, MQTT-3.3.1-4) followed, in the same write,
    // by PINGREQs that must not be answered after a DISCONNECT.
    let mut c = session(&broker);
    let mut p = crate::Publish::new(&topic(), b"x");
    p.qos = 3;
    p.packet_id = Some(1);
    let mut bytes = Packet::Publish(p).encode(V);
    for _ in 0..3 {
        bytes.extend(Packet::PingReq.encode(V));
    }
    let _ = c.send_raw(&bytes);
    // A Malformed Packet closes the connection (MQTT-4.13.1-1).
    // req: 5.0/MQTT-4.13.1-1
    seen |= disconnect_is_last(&mut c, "malformed packet");

    if !seen {
        crate::outcome::skipped("broker closed without sending DISCONNECT");
    }
}
