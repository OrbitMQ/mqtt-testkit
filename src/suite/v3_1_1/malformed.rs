// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Malformed packets and protocol violations after CONNECT (MQTT 3.1.1).

use crate::{Writer, broker_or_skip, frame};

use super::assert_violation_closes;

pub fn subscribe_with_invalid_fixed_header_flags_closes() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.u16(1).str("conformance/flags").u8(0);
    for first in [0x80, 0x83, 0x8A] {
        // req: 3.1.1/MQTT-3.8.1-1
        // req: 3.1.1/MQTT-2.2.2-1
        // req: 3.1.1/MQTT-2.2.2-2
        assert_violation_closes(&broker, &body.frame(first));
    }
}

pub fn unsubscribe_with_invalid_fixed_header_flags_closes() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.u16(1).str("conformance/flags");
    for first in [0xA0, 0xA3, 0xAA] {
        // req: 3.1.1/MQTT-3.10.1-1
        // req: 3.1.1/MQTT-2.2.2-2
        assert_violation_closes(&broker, &body.frame(first));
    }
}

pub fn pubrel_with_invalid_fixed_header_flags_closes() {
    let broker = broker_or_skip!();
    for first in [0x60, 0x63, 0x6A] {
        // req: 3.1.1/MQTT-3.6.1-1
        // req: 3.1.1/MQTT-2.2.2-2
        assert_violation_closes(&broker, &frame(first, &[0x00, 0x01]));
    }
}

pub fn pingreq_with_invalid_fixed_header_flags_closes() {
    let broker = broker_or_skip!();
    // req: 3.1.1/MQTT-2.2.2-2
    assert_violation_closes(&broker, &[0xC1, 0x00]);
}

pub fn disconnect_with_invalid_fixed_header_flags_closes() {
    let broker = broker_or_skip!();
    // req: 3.1.1/MQTT-3.14.1-1
    assert_violation_closes(&broker, &[0xE1, 0x00]);
}

pub fn publish_with_qos_3_closes() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.str("conformance/qos3").u16(1).raw(b"x");
    // req: 3.1.1/MQTT-3.3.1-4
    assert_violation_closes(&broker, &body.frame(0x36));
}

pub fn remaining_length_longer_than_four_bytes_closes() {
    let broker = broker_or_skip!();
    // req: 3.1.1/MQTT-4.8.0-1
    assert_violation_closes(&broker, &[0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
}

pub fn reserved_packet_types_close() {
    let broker = broker_or_skip!();
    for first in [0x00, 0xF0] {
        // req: 3.1.1/MQTT-4.8.0-1
        assert_violation_closes(&broker, &[first, 0x00]);
    }
}

pub fn server_to_client_packet_from_client_closes() {
    let broker = broker_or_skip!();
    // CONNACK and SUBACK are only ever sent by a server.
    for bytes in [
        &[0x20, 0x02, 0x00, 0x00][..],
        &[0x90, 0x03, 0x00, 0x01, 0x00][..],
    ] {
        // req: 3.1.1/MQTT-4.8.0-1
        assert_violation_closes(&broker, bytes);
    }
}

/// Strict: MQTT-2.3.1-1 binds the sender; closing relies on the server detecting it (MQTT-4.8.0-1).
pub fn zero_packet_identifier_closes() {
    let broker = broker_or_skip!();
    let mut publish = Writer::new();
    publish.str("conformance/pid").u16(0).raw(b"x");
    let mut subscribe = Writer::new();
    subscribe.u16(0).str("conformance/pid").u8(0);
    let mut unsubscribe = Writer::new();
    unsubscribe.u16(0).str("conformance/pid");
    for bytes in [
        publish.frame(0x32),
        subscribe.frame(0x82),
        unsubscribe.frame(0xA2),
    ] {
        // req: 3.1.1/MQTT-2.3.1-1
        assert_violation_closes(&broker, &bytes);
    }
}

/// Strict: MQTT-3.3.2-2 binds the sender; closing relies on the server detecting it (MQTT-4.8.0-1).
pub fn publish_topic_with_wildcards_closes() {
    let broker = broker_or_skip!();
    for t in ["conformance/+", "conformance/#", "+"] {
        let mut body = Writer::new();
        body.str(t).raw(b"x");
        // req: 3.1.1/MQTT-3.3.2-2
        // req: 3.1.1/MQTT-4.7.1-1
        assert_violation_closes(&broker, &body.frame(0x30));
    }
}

pub fn publish_topic_with_ill_formed_utf8_closes() {
    let broker = broker_or_skip!();
    for t in [&[b'a', 0xFF][..], &[0xED, 0xA0, 0x80][..]] {
        let mut body = Writer::new();
        body.bin(t).raw(b"x");
        // req: 3.1.1/MQTT-1.5.3-1
        // req: 3.1.1/MQTT-3.3.2-1
        assert_violation_closes(&broker, &body.frame(0x30));
    }
}

/// The spec calls a SUBSCRIBE without filters a protocol violation (MQTT-3.8.3-3),
/// which the server must close on (MQTT-4.8.0-1).
pub fn subscribe_without_topic_filters_closes() {
    let broker = broker_or_skip!();
    // req: 3.1.1/MQTT-3.8.3-3
    // req: 3.1.1/MQTT-4.8.0-1
    assert_violation_closes(&broker, &frame(0x82, &[0x00, 0x01]));
}

pub fn subscribe_with_invalid_requested_qos_closes() {
    let broker = broker_or_skip!();
    for qos in [0x03, 0x04, 0x80] {
        let mut body = Writer::new();
        body.u16(1).str("conformance/qos").u8(qos);
        // req: 3.1.1/MQTT-3.8.3-4
        assert_violation_closes(&broker, &body.frame(0x82));
    }
}

/// The spec calls an UNSUBSCRIBE without filters a protocol violation
/// (MQTT-3.10.3-2), which the server must close on (MQTT-4.8.0-1).
pub fn unsubscribe_without_topic_filters_closes() {
    let broker = broker_or_skip!();
    // req: 3.1.1/MQTT-3.10.3-2
    // req: 3.1.1/MQTT-4.8.0-1
    assert_violation_closes(&broker, &frame(0xA2, &[0x00, 0x01]));
}

pub fn subscribe_with_ill_formed_utf8_filter_closes() {
    let broker = broker_or_skip!();
    for f in [&[b'a', 0xFF][..], &[b'a', b'/', 0xED, 0xA0, 0x80][..]] {
        let mut body = Writer::new();
        body.u16(1).bin(f).u8(0);
        // req: 3.1.1/MQTT-3.8.3-1
        // req: 3.1.1/MQTT-1.5.3-1
        assert_violation_closes(&broker, &body.frame(0x82));
    }
}

pub fn unsubscribe_with_ill_formed_utf8_filter_closes() {
    let broker = broker_or_skip!();
    for f in [&[b'a', 0xFF][..], &[b'a', b'/', 0xED, 0xA0, 0x80][..]] {
        let mut body = Writer::new();
        body.u16(1).bin(f);
        // req: 3.1.1/MQTT-3.10.3-1
        // req: 3.1.1/MQTT-1.5.3-1
        assert_violation_closes(&broker, &body.frame(0xA2));
    }
}
