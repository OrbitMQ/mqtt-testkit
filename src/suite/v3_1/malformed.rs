// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Malformed packets and fixed-header rules (MQTT 3.1 sections 2.1, 3.x).

use crate::{Packet, Writer, broker_or_skip, default_timeout, frame, unique_id};

use super::{assert_violation_closes, session};

pub fn publish_with_qos_3_closes() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.str("conformance/qos3").u16(1).raw(b"x");
    // req: 3.1/V31-2.1-5
    assert_violation_closes(&broker, &body.frame(0x36));
}

pub fn remaining_length_longer_than_four_bytes_closes() {
    let broker = broker_or_skip!();
    // req: 3.1/V31-2.1-2
    assert_violation_closes(&broker, &[0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
}

pub fn reserved_message_types_close() {
    let broker = broker_or_skip!();
    for first in [0x00, 0xF0] {
        // req: 3.1/V31-2.1-1
        assert_violation_closes(&broker, &[first, 0x00]);
    }
}

pub fn subscribe_with_requested_qos_3_closes() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.u16(1).str("conformance/qos").u8(0x03);
    // req: 3.1/V31-3.8-4
    assert_violation_closes(&broker, &body.frame(0x82));
}

pub fn publish_topic_with_wildcards_closes() {
    let broker = broker_or_skip!();
    for t in ["conformance/+", "conformance/#"] {
        let mut body = Writer::new();
        body.str(t).raw(b"x");
        // req: 3.1/V31-3.3-1
        // req: 3.1/V31-A-1
        assert_violation_closes(&broker, &body.frame(0x30));
    }
}

pub fn subscribe_and_unsubscribe_with_dup_set_are_accepted() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    let t = format!("conformance/{}", unique_id("dup"));
    let mut sub = Writer::new();
    sub.u16(7).str(&t).u8(1);
    c.send_raw(&sub.frame(0x8A)).unwrap();
    // req: 3.1/V31-3.8-1
    // req: 3.1/V31-2.1-3
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::SubAck(a) => assert_eq!(a.packet_id, 7),
        p => panic!("expected SUBACK, got {p:?}"),
    }
    let mut unsub = Writer::new();
    unsub.u16(8).str(&t);
    c.send_raw(&unsub.frame(0xAA)).unwrap();
    // req: 3.1/V31-3.10-1
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::UnsubAck(a) => assert_eq!(a.packet_id, 8),
        p => panic!("expected UNSUBACK, got {p:?}"),
    }
}

pub fn pubrel_with_dup_set_is_accepted() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    let mut publish = Writer::new();
    publish
        .str(&format!("conformance/{}", unique_id("rel")))
        .u16(9)
        .raw(b"x");
    c.send_raw(&publish.frame(0x34)).unwrap();
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubRec(a) => assert_eq!(a.packet_id, 9),
        p => panic!("expected PUBREC, got {p:?}"),
    }
    // Retried PUBREL: QoS 1 in the fixed header plus DUP (0x6A).
    c.send_raw(&frame(0x6A, &[0x00, 0x09])).unwrap();
    // req: 3.1/V31-3.6-1
    // req: 3.1/V31-3.6-2
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubComp(a) => assert_eq!(a.packet_id, 9),
        p => panic!("expected PUBCOMP, got {p:?}"),
    }
}
