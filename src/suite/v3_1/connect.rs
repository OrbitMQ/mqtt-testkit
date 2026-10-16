// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! CONNECT / CONNACK validation (MQTT 3.1 sections 2.2, 3.1, 3.2).

use crate::{Connect, Packet, Will, broker_or_skip, default_timeout, unique_id};

use super::{V, refused, session};

pub fn mqisdp_level_3_is_accepted() {
    let broker = broker_or_skip!();
    let connect = Connect::new(V, &unique_id("acc"));
    assert_eq!(
        (connect.protocol_name.as_str(), connect.protocol_level),
        ("MQIsdp", 3)
    );
    // The harness decoder rejects a 3.x CONNACK whose remaining length is not 2.
    // req: 3.1/V31-3.2-1
    let (mut c, ack) = broker.connect(&connect).unwrap();
    // req: 3.1/V31-2.2-1
    // req: 3.1/V31-3.2-5
    assert_eq!(ack.code, 0x00);
    // The reserved CONNACK byte is checked (as a SHOULD) by
    // server_sends_unused_bits_as_zero.
    c.ping().unwrap();
}

/// Strict: the version byte 3 binds the client (V31-2.2-2); for a CONNECT with an
/// invalid version 3.1 says only that the server should close (V31-3.1-10).
pub fn mqisdp_with_another_level_is_refused() {
    let broker = broker_or_skip!();
    for level in [4u8, 5, 2] {
        let connect = Connect::new(V, &unique_id("lvl")).protocol("MQIsdp", level);
        if level == 5 {
            // Level 5 (sent with a 5.0 Property Length) may be read as a 5.0
            // CONNECT with a foreign protocol name, which a 5.0 server may
            // refuse with a 5.0 CONNACK and any error code (MQTT-3.1.2-1).
            let mut c = broker.open(crate::Version::V5).unwrap();
            c.send(&Packet::Connect(Box::new(connect.clone()))).unwrap();
            // req: 3.1/V31-2.2-2
            // req: 3.1/V31-3.1-10
            let ack = c
                .expect_refused(default_timeout())
                .unwrap_or_else(|e| panic!("CONNECT {connect:?}: {e}"));
            if let Some(ack) = ack {
                crate::outcome::note(format!(
                    "MQIsdp level 5 refused with code {:#04x}",
                    ack.code
                ));
            }
            continue;
        }
        // req: 3.1/V31-2.2-2
        // req: 3.1/V31-3.1-10
        if let Some(ack) = refused(&broker, &connect) {
            // req: 3.1/V31-3.1-11
            // req: 3.1/V31-3.2-4
            assert_eq!(ack.code, 0x01, "MQIsdp level {level}");
        }
    }
}

pub fn client_id_longer_than_23_characters_gets_code_2() {
    let broker = broker_or_skip!();
    let mut id = unique_id("long");
    while id.len() < 24 {
        id.push('x');
    }
    // req: 3.1/V31-3.2-4
    let ack = refused(&broker, &Connect::new(V, &id)).expect("closed without CONNACK 2");
    // req: 3.1/V31-3.1-4
    assert_eq!(ack.code, 0x02);
}

pub fn empty_client_id_gets_code_2() {
    let broker = broker_or_skip!();
    let ack = refused(&broker, &Connect::new(V, "")).expect("closed without CONNACK 2");
    // req: 3.1/V31-3.2-2
    assert_eq!(ack.code, 0x02);
}

pub fn client_id_of_23_characters_is_accepted() {
    let broker = broker_or_skip!();
    let mut id = unique_id("max");
    while id.len() < 23 {
        id.push('y');
    }
    let (_c, ack) = broker.connect(&Connect::new(V, &id)).unwrap();
    // req: 3.1/V31-3.1-3
    assert_eq!(ack.code, 0x00);
}

/// Strict: "It is not valid to supply a password without supplying a user name" binds the client; 3.1 says only that the server should close an invalid CONNECT (V31-3.1-10).
pub fn password_without_username_is_refused() {
    let broker = broker_or_skip!();
    let connect = Connect::new(V, &unique_id("pw")).password(b"secret");
    // req: 3.1/V31-2.2-13
    refused(&broker, &connect);
}

/// Strict: Will QoS 0-2 binds the client; 3.1 says only that the server should close an invalid CONNECT (V31-3.1-10).
pub fn will_qos_3_is_refused() {
    let broker = broker_or_skip!();
    let mut will = Will::new("conformance/will", b"x");
    will.qos = 3;
    // req: 3.1/V31-2.2-11
    refused(&broker, &Connect::new(V, &unique_id("w3")).will(will));
}

pub fn will_qos_and_retain_are_disregarded_without_will_flag() {
    let broker = broker_or_skip!();
    for flags in [0x0A, 0x22, 0x3A] {
        let (mut c, ack) = broker
            .connect(&Connect::new(V, &unique_id("wq")).flags(flags))
            .unwrap();
        // req: 3.1/V31-2.2-10
        assert_eq!(ack.code, 0x00, "flags {flags:#04x}");
        c.ping().unwrap();
    }
}

pub fn username_flag_without_username_string_is_accepted() {
    let broker = broker_or_skip!();
    let (mut c, ack) = broker
        .connect(&Connect::new(V, &unique_id("uf")).flags(0x82))
        .unwrap();
    // req: 3.1/V31-3.1-6
    assert_eq!(ack.code, 0x00);
    c.ping().unwrap();
}

pub fn password_flag_without_password_string_is_accepted() {
    let broker = broker_or_skip!();
    let connect = Connect::new(V, &unique_id("pf"))
        .username("user")
        .flags(0xC2);
    let (mut c, ack) = broker.connect(&connect).unwrap();
    // req: 3.1/V31-3.1-7
    assert_eq!(ack.code, 0x00);
    c.ping().unwrap();
}

pub fn connecting_with_a_connected_client_id_disconnects_the_old_client() {
    let broker = broker_or_skip!();
    let id = unique_id("take");
    let (mut old, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    let (mut new, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    // req: 3.1/V31-3.1-9
    old.expect_closed(default_timeout()).unwrap();
    new.ping().unwrap();
}

/// Strict: V31-3.1-1 binds the client; 3.1 states no server reaction.
pub fn first_packet_other_than_connect_closes() {
    let broker = broker_or_skip!();
    let publish = crate::Publish::new("conformance/first", b"x");
    for packet in [Packet::PingReq, Packet::Publish(publish)] {
        let mut c = broker.open(V).unwrap();
        c.send(&packet).unwrap();
        // No deadline for the close: a server may act only at its CONNECT timeout.
        // req: 3.1/V31-3.1-1
        let packets = c.expect_closed_eventually().unwrap();
        assert!(
            packets.is_empty(),
            "{} answered with {packets:?}",
            packet.name()
        );
    }
}

pub fn a_session_is_usable_after_connack() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    let t = format!("conformance/{}", unique_id("t"));
    // `subscribe` requires a SUBACK echoing the SUBSCRIBE's Message ID.
    // req: 3.1/V31-3.9-1
    let ack = c.subscribe(&t, 1).unwrap();
    // req: 3.1/V31-3.9-2
    assert_eq!(ack.codes.len(), 1);
    // req: 3.1/V31-3.9-3
    assert!(ack.codes.iter().all(|q| *q <= 2), "{ack:?}");
}

pub fn connack_return_codes_are_in_range() {
    let broker = broker_or_skip!();
    let mut long = unique_id("long");
    while long.len() < 24 {
        long.push('x');
    }
    let attempts = [
        Connect::new(V, &unique_id("ok")),
        Connect::new(V, &unique_id("lvl")).protocol("MQIsdp", 4),
        Connect::new(V, &long),
        Connect::new(V, ""),
        Connect::new(V, &unique_id("cred"))
            .username("conformance-nobody")
            .password(b"conformance-wrong"),
    ];
    for connect in attempts {
        let mut c = broker.open(V).unwrap();
        c.send(&Packet::Connect(Box::new(connect.clone()))).unwrap();
        match c.next_event(default_timeout()).unwrap() {
            crate::Event::Packet(Packet::ConnAck(ack)) => {
                // req: 3.1/V31-2.2-17
                assert!(ack.code <= 5, "{connect:?} answered with code {}", ack.code);
            }
            crate::Event::Closed => {}
            other => panic!("{connect:?}: {other:?}"),
        }
    }
}

pub fn bad_credentials_are_refused_with_code_4_or_5() {
    let broker = broker_or_skip!();
    let connect = Connect::new(V, &unique_id("auth"))
        .username("conformance-nobody")
        .password(unique_id("wrong").as_bytes());
    let (mut c, ack) = broker.connect(&connect).unwrap();
    if ack.code == 0 {
        c.disconnect().unwrap();
        crate::outcome::skipped("broker accepts any user name and password (no authentication)");
        return;
    }
    // req: 3.1/V31-3.2-3
    assert!(
        matches!(ack.code, 4 | 5),
        "credentials refused with code {}",
        ack.code
    );
    // 3.1 does not require the server to close after a refusal.
    if c.expect_closed(default_timeout()).is_err() {
        crate::outcome::note("refused connection left for the client to close");
    }
}

pub fn server_sends_unused_bits_as_zero() {
    let broker = broker_or_skip!();
    // The client masks and records unused fixed-header bits in every packet
    // the broker sends (CONNACK, SUBACK, PUBACK, ...); checked at the end.
    let (mut c, ack) = broker
        .connect(&Connect::new(V, &unique_id("bits")))
        .unwrap();
    // The first CONNACK byte is reserved in 3.1.
    // req: 3.1/V31-2.6-1
    assert_eq!(ack.flags, 0, "{ack:?}");
    let t = format!("conformance/{}", unique_id("b"));
    // QoS 1 keeps the deliveries' flows short; the QoS 2 PUBLISH below
    // still exercises the server's PUBREC and PUBCOMP.
    let granted = c.subscribe(&t, 1).unwrap();
    // The upper six bits of each granted QoS byte are unused.
    // req: 3.1/V31-2.6-1
    assert!(granted.codes.iter().all(|q| q & 0xFC == 0), "{granted:?}");
    c.publish(&t, b"b", 1, false).unwrap();
    c.publish(&t, b"b", 2, false).unwrap();
    // req: 3.1/V31-2.6-1
    let got = c.expect_publishes(2, default_timeout()).unwrap();
    assert_eq!(got.len(), 2);
    c.unsubscribe(&t).unwrap();
    c.ping().unwrap();
    // req: 3.1/V31-2.6-1
    assert!(
        !c.saw_unused_bits(),
        "unused fixed-header bits set (see notes)"
    );
}
