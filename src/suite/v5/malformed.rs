// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Malformed packets and protocol errors after CONNECT (MQTT 5.0).

use crate::{PropValue, Property, Writer, broker_or_skip, find_prop, frame, prop};

use super::{assert_error_closes, session_with_ack};

pub fn subscribe_with_invalid_fixed_header_flags_closes() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.u16(1).properties(&[]).str("conformance/flags").u8(0);
    for first in [0x80, 0x83, 0x8A] {
        // req: 5.0/MQTT-3.8.1-1
        // req: 5.0/MQTT-2.1.3-1
        assert_error_closes(&broker, &body.frame(first));
    }
}

pub fn unsubscribe_with_invalid_fixed_header_flags_closes() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.u16(1).properties(&[]).str("conformance/flags");
    for first in [0xA0, 0xA3, 0xAA] {
        // req: 5.0/MQTT-3.10.1-1
        // req: 5.0/MQTT-2.1.3-1
        assert_error_closes(&broker, &body.frame(first));
    }
}

pub fn pubrel_with_invalid_fixed_header_flags_closes() {
    let broker = broker_or_skip!();
    for first in [0x60, 0x63, 0x6A] {
        // req: 5.0/MQTT-3.6.1-1
        // req: 5.0/MQTT-2.1.3-1
        assert_error_closes(&broker, &frame(first, &[0x00, 0x01]));
    }
}

/// A DISCONNECT with reserved flags set is a Malformed Packet, not a normal
/// disconnect: the server closes and, unlike after DISCONNECT 0x00,
/// publishes the Will (MQTT-3.1.2-8).
pub fn disconnect_with_invalid_fixed_header_flags_closes() {
    let broker = broker_or_skip!();
    let t = super::topic();
    let mut sub = super::session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (mut c, _) = broker
        .connect_ok(
            &crate::Connect::new(super::V, &crate::unique_id("dfl"))
                .will(crate::Will::new(&t, b"bad-disconnect")),
        )
        .unwrap();
    c.send_raw(&[0xE1, 0x00]).unwrap();
    // req: 5.0/MQTT-3.14.1-1
    let reason = c
        .expect_error_close(crate::default_timeout())
        .unwrap_or_else(|e| panic!("after e1 00: {e}"));
    if let Some(r) = reason.filter(|r| *r != 0x81) {
        crate::outcome::note(format!("DISCONNECT {r:#04x}, not 0x81 (Malformed Packet)"));
    }
    // req: 5.0/MQTT-3.14.1-1
    // req: 5.0/MQTT-3.1.2-8
    let p = sub.expect_publish(crate::default_timeout()).unwrap();
    assert_eq!(p.payload, b"bad-disconnect");
}

/// AUTH with a reserved fixed-header bit set is malformed whatever its content,
/// so this needs no supported Authentication Method.
pub fn auth_with_invalid_fixed_header_flags_closes() {
    let broker = broker_or_skip!();
    // Reason 0x19 (Re-authenticate), empty Property Length; flags 0001.
    // req: 5.0/MQTT-3.15.1-1
    // req: 5.0/MQTT-2.1.3-1
    assert_error_closes(&broker, &frame(0xF1, &[0x19, 0x00]));
}

pub fn publish_with_qos_3_is_malformed() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.str("conformance/qos3")
        .u16(1)
        .properties(&[])
        .raw(b"x");
    // req: 5.0/MQTT-3.3.1-4
    let reason = assert_error_closes(&broker, &body.frame(0x36));
    // "Use DISCONNECT with Reason Code 0x81" (§3.3.1.2) under a SHOULD-level
    // DISCONNECT (§4.13.1): another error code is noted.
    if let Some(r) = reason.filter(|r| *r != 0x81) {
        crate::outcome::note(format!("DISCONNECT {r:#04x}, not 0x81 (Malformed Packet)"));
    }
}

pub fn remaining_length_longer_than_four_bytes_closes() {
    let broker = broker_or_skip!();
    // req: 5.0/MQTT-4.13.1-1
    assert_error_closes(&broker, &[0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
}

/// Strict: MQTT-1.5.5-1 binds only the encoder; the spec never calls a non-minimal encoding malformed.
pub fn non_minimal_variable_byte_integer_closes() {
    let broker = broker_or_skip!();
    // PINGREQ whose Remaining Length 0 is encoded in two bytes.
    assert_error_closes(&broker, &[0xC0, 0x80, 0x00]);
}

pub fn reserved_packet_type_closes() {
    let broker = broker_or_skip!();
    // req: 5.0/MQTT-4.13.1-1
    assert_error_closes(&broker, &[0x00, 0x00]);
}

/// Strict: MQTT-2.2.1-3 binds the client; closing relies on the server detecting it (MQTT-4.13.1-1).
pub fn zero_packet_identifier_closes() {
    let broker = broker_or_skip!();
    let mut publish = Writer::new();
    publish
        .str("conformance/pid")
        .u16(0)
        .properties(&[])
        .raw(b"x");
    let mut subscribe = Writer::new();
    subscribe
        .u16(0)
        .properties(&[])
        .str("conformance/pid")
        .u8(0);
    for bytes in [publish.frame(0x32), subscribe.frame(0x82)] {
        // req: 5.0/MQTT-2.2.1-3
        assert_error_closes(&broker, &bytes);
    }
}

/// Strict: MQTT-3.3.2-2 / MQTT-4.7.0-1 bind the sender; closing relies on detection (MQTT-4.13.1-1).
pub fn publish_topic_with_wildcards_closes() {
    let broker = broker_or_skip!();
    for t in ["conformance/+", "conformance/#"] {
        let mut body = Writer::new();
        body.str(t).properties(&[]).raw(b"x");
        // req: 5.0/MQTT-3.3.2-2
        // req: 5.0/MQTT-4.7.0-1
        assert_error_closes(&broker, &body.frame(0x30));
    }
}

/// Strict: MQTT-3.3.2-14 binds the sender; closing relies on detection (MQTT-4.13.1-1).
pub fn response_topic_with_wildcards_closes() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.str("conformance/req")
        .properties(&[Property::new(
            prop::RESPONSE_TOPIC,
            PropValue::Str("conformance/+".into()),
        )])
        .raw(b"x");
    // req: 5.0/MQTT-3.3.2-14
    assert_error_closes(&broker, &body.frame(0x30));
}

pub fn user_property_with_ill_formed_utf8_closes() {
    let broker = broker_or_skip!();
    let mut props = Writer::new();
    props
        .varint(u32::from(prop::USER_PROPERTY))
        .bin(&[0xC3, 0x28])
        .str("v");
    let mut body = Writer::new();
    body.str("conformance/up")
        .varint(u32::try_from(props.as_bytes().len()).unwrap())
        .raw(props.as_bytes())
        .raw(b"x");
    // req: 5.0/MQTT-1.5.7-1
    assert_error_closes(&broker, &body.frame(0x30));
}

/// Strict: MQTT-3.3.4-6 binds the client; closing relies on detection (MQTT-4.13.1-1).
pub fn publish_with_subscription_identifier_closes() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.str("conformance/sid")
        .properties(&[Property::new(
            prop::SUBSCRIPTION_IDENTIFIER,
            PropValue::VarInt(1),
        )])
        .raw(b"x");
    // req: 5.0/MQTT-3.3.4-6
    assert_error_closes(&broker, &body.frame(0x30));
}

/// "A Topic Alias value of 0 … is a Protocol Error" (§3.3.4): the server must
/// close (MQTT-4.13.1-1).
pub fn topic_alias_zero_closes() {
    let broker = broker_or_skip!();
    let mut body = Writer::new();
    body.str("conformance/alias")
        .properties(&[Property::new(prop::TOPIC_ALIAS, PropValue::TwoByte(0))])
        .raw(b"x");
    // req: 5.0/MQTT-3.3.2-8
    // req: 5.0/MQTT-4.13.1-1
    assert_error_closes(&broker, &body.frame(0x30));
}

/// "A Topic Alias value … greater than the Maximum Topic Alias is a Protocol
/// Error" (§3.3.4): the server must close (MQTT-4.13.1-1).
pub fn topic_alias_above_maximum_closes() {
    let broker = broker_or_skip!();
    let (mut c, ack) = session_with_ack(&broker);
    let max = match find_prop(&ack.properties, prop::TOPIC_ALIAS_MAXIMUM) {
        None => 0,
        Some(PropValue::TwoByte(m)) => *m,
        Some(other) => panic!("bad Topic Alias Maximum {other:?}"),
    };
    let Some(alias) = max.checked_add(1) else {
        crate::outcome::skipped("Topic Alias Maximum is 65535");
        return;
    };
    let mut body = Writer::new();
    body.str("conformance/alias")
        .properties(&[Property::new(prop::TOPIC_ALIAS, PropValue::TwoByte(alias))])
        .raw(b"x");
    c.send_raw(&body.frame(0x30)).unwrap();
    // req: 5.0/MQTT-3.2.2-17
    // req: 5.0/MQTT-3.3.2-9
    // req: 5.0/MQTT-4.13.1-1
    let reason = c.expect_error_close(crate::default_timeout()).unwrap();
    // DISCONNECT is optional (a SHOULD, §4.13.1); 0x94 is the code the spec
    // names, so another one is noted (the strict flow case requires 0x94).
    if let Some(r) = reason.filter(|r| *r != 0x94) {
        crate::outcome::note(format!(
            "alias {alias} with maximum {max}: DISCONNECT {r:#04x}, not 0x94"
        ));
    }
}

/// "A SUBSCRIBE packet with no Payload is a Protocol Error" (§3.8.3): the server
/// must close (MQTT-4.13.1-1).
pub fn subscribe_without_topic_filters_closes() {
    let broker = broker_or_skip!();
    // req: 5.0/MQTT-3.8.3-2
    // req: 5.0/MQTT-4.13.1-1
    assert_error_closes(&broker, &frame(0x82, &[0x00, 0x01, 0x00]));
}

pub fn subscription_options_with_reserved_bits_close() {
    let broker = broker_or_skip!();
    for options in [0x40, 0x80, 0xC1] {
        let mut body = Writer::new();
        body.u16(1)
            .properties(&[])
            .str("conformance/opts")
            .u8(options);
        // req: 5.0/MQTT-3.8.3-5
        assert_error_closes(&broker, &body.frame(0x82));
    }
}

/// "An UNSUBSCRIBE packet with no Payload is a Protocol Error" (§3.10.3): the
/// server must close (MQTT-4.13.1-1).
pub fn unsubscribe_without_topic_filters_closes() {
    let broker = broker_or_skip!();
    // req: 5.0/MQTT-3.10.3-2
    // req: 5.0/MQTT-4.13.1-1
    assert_error_closes(&broker, &frame(0xA2, &[0x00, 0x01, 0x00]));
}

/// Ill-formed UTF-8: an invalid continuation and an encoded surrogate.
const BAD_UTF8: [&[u8]; 2] = [&[b'a', 0xC3, 0x28], &[0xED, 0xA0, 0x80]];

/// A QoS 0 PUBLISH whose only property is string property `id` with the
/// raw value `value`.
fn publish_with_raw_string_property(id: u8, value: &[u8]) -> Vec<u8> {
    let mut props = Writer::new();
    props.varint(u32::from(id)).bin(value);
    let mut body = Writer::new();
    body.str("conformance/utf8")
        .varint(u32::try_from(props.as_bytes().len()).unwrap())
        .raw(props.as_bytes())
        .raw(b"x");
    body.frame(0x30)
}

pub fn response_topic_with_ill_formed_utf8_closes() {
    let broker = broker_or_skip!();
    for value in BAD_UTF8 {
        // req: 5.0/MQTT-3.3.2-13
        // req: 5.0/MQTT-1.5.4-1
        // req: 5.0/MQTT-4.13.1-1
        assert_error_closes(
            &broker,
            &publish_with_raw_string_property(prop::RESPONSE_TOPIC, value),
        );
    }
}

pub fn content_type_with_ill_formed_utf8_closes() {
    let broker = broker_or_skip!();
    for value in BAD_UTF8 {
        // req: 5.0/MQTT-3.3.2-19
        // req: 5.0/MQTT-1.5.4-1
        // req: 5.0/MQTT-4.13.1-1
        assert_error_closes(
            &broker,
            &publish_with_raw_string_property(prop::CONTENT_TYPE, value),
        );
    }
}

pub fn subscribe_filter_with_ill_formed_utf8_closes() {
    let broker = broker_or_skip!();
    for filter in BAD_UTF8 {
        let mut body = Writer::new();
        body.u16(1).properties(&[]).bin(filter).u8(0);
        // An encoding error is a Malformed Packet, not a refused filter, so
        // a SUBACK failure code is not enough.
        // req: 5.0/MQTT-3.8.3-1
        // req: 5.0/MQTT-1.5.4-1
        // req: 5.0/MQTT-4.13.1-1
        assert_error_closes(&broker, &body.frame(0x82));
    }
}

pub fn unsubscribe_filter_with_ill_formed_utf8_closes() {
    let broker = broker_or_skip!();
    for filter in BAD_UTF8 {
        let mut body = Writer::new();
        body.u16(1).properties(&[]).bin(filter);
        // req: 5.0/MQTT-3.10.3-1
        // req: 5.0/MQTT-1.5.4-1
        // req: 5.0/MQTT-4.13.1-1
        assert_error_closes(&broker, &body.frame(0xA2));
    }
}
