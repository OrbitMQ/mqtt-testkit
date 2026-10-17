// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! CONNECT / CONNACK validation (MQTT 5.0 sections 3.1, 3.2, 4.12).

use crate::{
    Connect, Packet, PropValue, ReasonOnly, Will, Writer, broker_or_skip, default_timeout,
    find_prop, prop, unique_id,
};

use super::{V, refused, session};

pub fn valid_connect_is_accepted_with_connack_first() {
    let broker = broker_or_skip!();
    // `connect` fails unless the first packet from the broker is CONNACK.
    // req: 5.0/MQTT-3.2.0-1
    let (mut c, ack) = broker.connect(&Connect::new(V, &unique_id("acc"))).unwrap();
    // req: 5.0/MQTT-3.1.4-5
    assert_eq!(ack.code, 0x00);
    // req: 5.0/MQTT-3.2.2-2
    assert!(!ack.session_present());
    // req: 5.0/MQTT-3.2.2-1
    assert_eq!(ack.flags & 0xFE, 0);
    c.ping().unwrap();
}

pub fn client_id_of_23_alphanumerics_is_accepted() {
    let broker = broker_or_skip!();
    let mut id = unique_id("Z9");
    while id.len() < 23 {
        id.push('a');
    }
    let (_c, ack) = broker.connect(&Connect::new(V, &id)).unwrap();
    // req: 5.0/MQTT-3.1.3-5
    assert_eq!(ack.code, 0x00);
}

pub fn unsupported_protocol_version_is_refused_with_0x84() {
    let broker = broker_or_skip!();
    for level in [6u8, 7, 0xFF] {
        let connect = Connect::new(V, &unique_id("lvl")).protocol("MQTT", level);
        // req: 5.0/MQTT-3.1.2-2
        // req: 5.0/MQTT-3.2.2-7
        // A CONNACK, if any, is 0x84 or (from a server that answers under
        // MQTT 3.1.1 rules) 0x01; `refused` checks which.
        if let Some(ack) = refused(&broker, &connect) {
            // req: 5.0/MQTT-3.2.2-6
            assert!(!ack.session_present());
        }
    }
}

pub fn foreign_protocol_name_is_refused() {
    let broker = broker_or_skip!();
    for name in ["MQTX", "mqtt", "MQIsdp"] {
        let connect = Connect::new(V, &unique_id("name")).protocol(name, 5);
        let mut c = broker.open(V).unwrap();
        c.send(&Packet::Connect(Box::new(connect))).unwrap();
        // A close, a non-zero CONNACK then a close, or silence (the bytes
        // handled under another protocol); never accepted as MQTT.
        // req: 5.0/MQTT-3.1.2-1
        let what = c
            .expect_foreign_protocol_not_accepted()
            .unwrap_or_else(|e| panic!("protocol name {name:?}: {e}"));
        crate::outcome::note(format!("protocol name {name:?}: {what}"));
    }
}

pub fn reserved_connect_flag_is_a_malformed_packet() {
    let broker = broker_or_skip!();
    // req: 5.0/MQTT-3.1.2-3
    // req: 5.0/MQTT-3.1.4-1
    // req: 5.0/MQTT-4.13.1-1
    refused(&broker, &Connect::new(V, &unique_id("rsv")).flags(0x03));
}

pub fn will_qos_without_will_flag_is_refused() {
    let broker = broker_or_skip!();
    for flags in [0x0A, 0x12] {
        // req: 5.0/MQTT-3.1.2-11
        // req: 5.0/MQTT-3.1.4-1
        refused(&broker, &Connect::new(V, &unique_id("wq")).flags(flags));
    }
}

pub fn will_retain_without_will_flag_is_refused() {
    let broker = broker_or_skip!();
    // req: 5.0/MQTT-3.1.2-13
    // req: 5.0/MQTT-3.1.4-1
    refused(&broker, &Connect::new(V, &unique_id("wr")).flags(0x22));
}

pub fn will_qos_3_is_refused() {
    let broker = broker_or_skip!();
    let mut will = Will::new("conformance/will", b"x");
    will.qos = 3;
    // req: 5.0/MQTT-3.1.2-12
    refused(&broker, &Connect::new(V, &unique_id("w3")).will(will));
}

pub fn will_flag_without_will_fields_is_refused() {
    let broker = broker_or_skip!();
    // req: 5.0/MQTT-3.1.2-9
    // req: 5.0/MQTT-3.1.4-1
    refused(&broker, &Connect::new(V, &unique_id("wf")).flags(0x06));
}

pub fn username_flag_without_username_is_refused() {
    let broker = broker_or_skip!();
    // req: 5.0/MQTT-3.1.2-17
    // req: 5.0/MQTT-3.1.4-1
    refused(&broker, &Connect::new(V, &unique_id("uf")).flags(0x82));
}

pub fn username_present_without_flag_is_refused() {
    let broker = broker_or_skip!();
    let connect = Connect::new(V, &unique_id("un"))
        .username("user")
        .flags(0x02);
    // req: 5.0/MQTT-3.1.2-16
    // req: 5.0/MQTT-3.1.4-1
    refused(&broker, &connect);
}

pub fn password_flag_without_password_is_refused() {
    let broker = broker_or_skip!();
    let connect = Connect::new(V, &unique_id("pf"))
        .username("user")
        .flags(0xC2);
    // req: 5.0/MQTT-3.1.2-19
    // req: 5.0/MQTT-3.1.4-1
    refused(&broker, &connect);
}

pub fn password_present_without_flag_is_refused() {
    let broker = broker_or_skip!();
    let connect = Connect::new(V, &unique_id("pn"))
        .username("user")
        .password(b"pw")
        .flags(0x82);
    // req: 5.0/MQTT-3.1.2-18
    // req: 5.0/MQTT-3.1.4-1
    refused(&broker, &connect);
}

pub fn empty_client_id_gets_an_assigned_client_identifier() {
    let broker = broker_or_skip!();
    let mut a = broker.open(V).unwrap();
    a.send(&Packet::Connect(Box::new(Connect::new(V, ""))))
        .unwrap();
    let ack_a = match a.next_event(default_timeout()).unwrap() {
        crate::Event::Packet(Packet::ConnAck(ack)) if ack.code == 0 => ack,
        other => {
            // Refused: a CONNACK error code (0x85 is a MAY) then close, or a
            // bare close.
            // req: 5.0/MQTT-3.1.3-8
            match other {
                crate::Event::Packet(Packet::ConnAck(ack)) => {
                    assert!(
                        super::CONNACK_CODES.contains(&ack.code) && ack.code >= 0x80,
                        "{ack:?}"
                    );
                    if ack.code != 0x85 {
                        crate::outcome::note(format!(
                            "empty client id refused with {:#04x}",
                            ack.code
                        ));
                    }
                    a.expect_closed(default_timeout()).unwrap();
                }
                crate::Event::Closed => {}
                other => panic!("expected CONNACK or a close, got {other:?}"),
            }
            crate::outcome::note("broker refuses an empty client identifier");
            return;
        }
    };
    let (mut b, ack_b) = broker.connect_ok(&Connect::new(V, "")).unwrap();
    let assigned =
        |ack: &crate::ConnAck| match find_prop(&ack.properties, prop::ASSIGNED_CLIENT_IDENTIFIER) {
            Some(PropValue::Str(s)) => s.clone(),
            other => panic!("no Assigned Client Identifier in {ack:?} ({other:?})"),
        };
    // req: 5.0/MQTT-3.1.3-7
    // req: 5.0/MQTT-3.2.2-16
    let (id_a, id_b) = (assigned(&ack_a), assigned(&ack_b));
    // req: 5.0/MQTT-3.1.3-6
    assert_ne!(id_a, "", "assigned client identifier must be non-empty");
    assert_ne!(id_a, id_b);
    a.ping().unwrap();
    b.ping().unwrap();
}

fn raw_connect_with_client_id(id: &[u8]) -> Vec<u8> {
    let mut w = Writer::new();
    w.str("MQTT").u8(5).u8(0x02).u16(60).properties(&[]).bin(id);
    w.frame(0x10)
}

pub fn ill_formed_utf8_client_id_is_refused() {
    let broker = broker_or_skip!();
    for id in [&[b'a', 0xC3, 0x28][..], &[0xED, 0xA0, 0x80][..]] {
        // req: 5.0/MQTT-1.5.4-1
        // req: 5.0/MQTT-3.1.3-4
        raw_refused(&broker, &raw_connect_with_client_id(id));
    }
}

pub fn null_character_in_client_id_is_refused() {
    let broker = broker_or_skip!();
    // req: 5.0/MQTT-1.5.4-2
    raw_refused(&broker, &raw_connect_with_client_id(b"ab\0cd"));
}

pub fn second_connect_is_a_protocol_error() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    c.send(&Packet::Connect(Box::new(Connect::new(
        V,
        &unique_id("dup"),
    ))))
    .unwrap();
    // `expect_error_close` fails on any packet other than one DISCONNECT, so
    // a second CONNACK is caught here.
    // req: 5.0/MQTT-3.1.0-2
    // req: 5.0/MQTT-3.2.0-2
    c.expect_error_close(default_timeout()).unwrap();
}

/// PINGREQ and a PUBLISH, each sent as the first packet of a connection.
fn first_packets() -> [Packet; 2] {
    [
        Packet::PingReq,
        Packet::Publish(crate::Publish::new("conformance/first", b"x")),
    ]
}

/// The server answers a first packet other than CONNECT with nothing but a
/// refusal (a CONNACK of 0x80 or above); never a DISCONNECT. It may close or
/// wait for a CONNECT.
pub fn first_packet_other_than_connect_is_not_answered() {
    let broker = broker_or_skip!();
    for packet in first_packets() {
        let mut c = broker.open(V).unwrap();
        c.send(&packet).unwrap();
        let (packets, _closed) = c.packets_until_close(default_timeout()).unwrap();
        // req: 5.0/MQTT-3.2.0-1
        // req: 5.0/MQTT-3.14.0-1
        assert!(
            matches!(
                packets.as_slice(),
                [] | [Packet::ConnAck(crate::ConnAck {
                    code: 0x80..=0xFF,
                    ..
                })]
            ),
            "{} answered with {packets:?}",
            packet.name()
        );
    }
}

/// Strict: MQTT-3.1.0-1 binds the client; the only server text is a SHOULD close
/// without CONNECT (3.1.4), so closing relies on detection (MQTT-4.13.1-1).
pub fn first_packet_other_than_connect_closes_without_disconnect() {
    let broker = broker_or_skip!();
    for packet in first_packets() {
        let mut c = broker.open(V).unwrap();
        c.send(&packet).unwrap();
        // No deadline for the close: a server may act only at its CONNECT timeout.
        // req: 5.0/MQTT-3.1.0-1
        let packets = c
            .expect_closed_eventually()
            .unwrap_or_else(|e| panic!("{}: {e}", packet.name()));
        assert!(
            packets.is_empty(),
            "{} answered with {packets:?}",
            packet.name()
        );
    }
}

pub fn connecting_with_a_connected_client_id_disconnects_the_old_client() {
    let broker = broker_or_skip!();
    let id = unique_id("take");
    let (mut old, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    let (mut new, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    // req: 5.0/MQTT-3.1.4-3
    let reason = old.expect_disconnect(default_timeout()).unwrap();
    assert!(matches!(reason, None | Some(0x8E)), "reason {reason:?}");
    new.ping().unwrap();
}

pub fn session_present_and_clean_start() {
    let broker = broker_or_skip!();
    let id = unique_id("sp");
    let persistent = Connect::new(V, &id)
        .clean(false)
        .property(prop::SESSION_EXPIRY_INTERVAL, PropValue::FourByte(60));

    let (c, ack) = broker.connect_ok(&persistent).unwrap();
    // req: 5.0/MQTT-3.1.2-6
    // req: 5.0/MQTT-3.2.2-3
    assert!(!ack.session_present(), "new session");
    c.disconnect().unwrap();

    let (c, ack) = broker.connect_ok(&persistent).unwrap();
    // req: 5.0/MQTT-3.1.2-5
    // req: 5.0/MQTT-3.1.2-23
    assert!(ack.session_present(), "resumed session");
    c.disconnect().unwrap();

    let (c, ack) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    // req: 5.0/MQTT-3.1.2-4
    assert!(!ack.session_present(), "clean start");
    c.disconnect().unwrap();
}

pub fn unsupported_authentication_method_is_refused() {
    let broker = broker_or_skip!();
    let connect = super::unknown_auth_method_connect();
    // req: 5.0/MQTT-4.12.0-1
    if let Some(ack) = refused(&broker, &connect) {
        assert!(matches!(ack.code, 0x8C | 0x87), "{ack:?}");
    }
}

pub fn no_authentication_method_or_auth_without_one_in_connect() {
    let broker = broker_or_skip!();
    let (mut c, ack) = super::session_with_ack(&broker);
    // req: 5.0/MQTT-4.12.0-6
    assert!(find_prop(&ack.properties, prop::AUTHENTICATION_METHOD).is_none());
    // No AUTH arrives either: the next packet is the PINGRESP.
    // req: 5.0/MQTT-4.12.0-6
    c.ping().unwrap();
}

/// "It is a Protocol Error for the Client or Server to send an AUTH packet if the
/// CONNECT packet did not contain the same Authentication Method" (§3.15): the
/// server must close (MQTT-4.13.1-1).
pub fn auth_without_authentication_method_is_a_protocol_error() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    c.send(&Packet::Auth(ReasonOnly {
        reason: Some(0x19),
        properties: Vec::new(),
    }))
    .unwrap();
    // req: 5.0/MQTT-4.12.0-7
    // req: 5.0/MQTT-4.13.1-1
    c.expect_error_close(default_timeout()).unwrap();
}

/// Sends `bytes` (a hand-built CONNECT) on a fresh connection and requires
/// a refusal; any CONNACK must carry a defined error code.
fn raw_refused(broker: &crate::Broker, bytes: &[u8]) {
    let mut c = broker.open(V).unwrap();
    c.send_raw(bytes).unwrap();
    let ack = c
        .expect_refused(default_timeout())
        .unwrap_or_else(|e| panic!("CONNECT {bytes:02x?}: {e}"));
    if let Some(a) = ack {
        assert!(super::CONNACK_CODES.contains(&a.code), "{a:?}");
        assert!(a.code >= 0x80, "{a:?}");
        // MQTT-3.2.2-6: a refusing CONNACK has Session Present 0.
        assert!(!a.session_present(), "{a:?}");
    }
}

pub fn connect_without_client_identifier_is_refused() {
    let broker = broker_or_skip!();
    // Variable header only: the payload, which must start with the Client
    // Identifier, is missing.
    let mut w = Writer::new();
    w.str("MQTT").u8(5).u8(0x02).u16(60).properties(&[]);
    // req: 5.0/MQTT-3.1.3-3
    // req: 5.0/MQTT-3.1.4-1
    raw_refused(&broker, &w.frame(0x10));
}

pub fn ill_formed_utf8_will_topic_is_refused() {
    let broker = broker_or_skip!();
    for topic in [&[b'w', 0xC3, 0x28][..], &[0xED, 0xA0, 0x80][..]] {
        let mut w = Writer::new();
        w.str("MQTT")
            .u8(5)
            .u8(0x06)
            .u16(60)
            .properties(&[])
            .str(&unique_id("wt"))
            .properties(&[])
            .bin(topic)
            .bin(b"will");
        // req: 5.0/MQTT-3.1.3-11
        // req: 5.0/MQTT-3.1.4-1
        // req: 5.0/MQTT-1.5.4-1
        raw_refused(&broker, &w.frame(0x10));
    }
}

pub fn ill_formed_utf8_user_name_is_refused() {
    let broker = broker_or_skip!();
    for name in [&[b'u', 0xC3, 0x28][..], &[0xED, 0xA0, 0x80][..]] {
        let mut w = Writer::new();
        w.str("MQTT")
            .u8(5)
            .u8(0x82)
            .u16(60)
            .properties(&[])
            .str(&unique_id("un"))
            .bin(name);
        // req: 5.0/MQTT-3.1.3-12
        // req: 5.0/MQTT-3.1.4-1
        // req: 5.0/MQTT-1.5.4-1
        raw_refused(&broker, &w.frame(0x10));
    }
}

pub fn data_after_a_refused_connect_is_ignored() {
    let broker = broker_or_skip!();
    let t = super::topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let mut bad_connects = vec![Connect::new(V, &unique_id("ign")).protocol("MQTT", 6)];
    if super::refuses_unknown_auth_method(&broker) {
        bad_connects.push(super::unknown_auth_method_connect());
    }
    for connect in bad_connects {
        // CONNECT, then (in the same write) a PUBLISH, a SUBSCRIBE and a
        // PINGREQ that a server processing them would answer.
        let mut bytes = Packet::Connect(Box::new(connect.clone())).encode(V);
        let mut p = crate::Publish::new(&t, b"after-refusal");
        p.qos = 1;
        p.packet_id = Some(1);
        bytes.extend(Packet::Publish(p).encode(V));
        bytes.extend(
            Packet::Subscribe(crate::Subscribe {
                packet_id: 2,
                properties: Vec::new(),
                filters: vec![(t.clone(), 0)],
            })
            .encode(V),
        );
        bytes.extend(Packet::PingReq.encode(V));
        let mut c = broker.open(V).unwrap();
        let _ = c.send_raw(&bytes);
        // Only a CONNACK (or nothing) before the close: no PUBACK, SUBACK
        // or PINGRESP.
        // req: 5.0/MQTT-3.1.4-6
        c.expect_refused(default_timeout())
            .unwrap_or_else(|e| panic!("{connect:?}: {e}"));
    }
    // req: 5.0/MQTT-3.1.4-6
    sub.expect_no_packet(crate::quiet_period()).unwrap();
}

pub fn connack_reason_codes_come_from_the_table() {
    let broker = broker_or_skip!();
    let (_c, ack) = broker.connect(&Connect::new(V, &unique_id("tbl"))).unwrap();
    // req: 5.0/MQTT-3.2.2-8
    assert!(super::CONNACK_CODES.contains(&ack.code), "{ack:?}");
    let mut will = Will::new("conformance/will", b"x");
    will.qos = 3;
    // Only genuine level-5 CONNECTs: another level may be answered under
    // another version's rules.
    let mut refusals = vec![
        Connect::new(V, &unique_id("tbl")).flags(0x03),
        Connect::new(V, &unique_id("tbl")).will(will),
    ];
    if super::refuses_unknown_auth_method(&broker) {
        refusals.push(super::unknown_auth_method_connect());
    }
    for connect in refusals {
        let mut c = broker.open(V).unwrap();
        c.send(&Packet::Connect(Box::new(connect.clone()))).unwrap();
        // A refusal (CONNACK >= 0x80 or not) must end with a close.
        // req: 5.0/MQTT-4.13.2-1
        let ack = c
            .expect_refused(default_timeout())
            .unwrap_or_else(|e| panic!("{connect:?}: {e}"));
        if let Some(a) = ack {
            // req: 5.0/MQTT-3.2.2-8
            assert!(super::CONNACK_CODES.contains(&a.code), "{a:?}");
        }
    }
}

/// The server's Maximum QoS (2 when absent). A present value must be 0 or 1.
fn maximum_qos(ack: &crate::ConnAck) -> u8 {
    match find_prop(&ack.properties, prop::MAXIMUM_QOS) {
        None => 2,
        Some(PropValue::Byte(q @ (0 | 1))) => *q,
        Some(other) => panic!("Maximum QoS {other:?} (must be 0 or 1)"),
    }
}

pub fn maximum_qos_is_announced_unless_qos_2_is_supported() {
    let broker = broker_or_skip!();
    let (mut c, ack) = super::session_with_ack(&broker);
    // A server limited to QoS 0 or 1 announces it (value 0 or 1).
    // req: 5.0/MQTT-3.2.2-9
    let max = maximum_qos(&ack);
    if max < 2 {
        crate::outcome::note(format!("broker announces Maximum QoS {max}"));
        return;
    }
    // No Maximum QoS: the server claims QoS 2, so it must accept QoS 1 and
    // QoS 2 PUBLISHes and complete their flows.
    let t = super::topic();
    for qos in [1, 2] {
        let mut p = crate::Publish::new(&t, b"q");
        p.qos = qos;
        let a = c.publish_packet(p).unwrap().unwrap();
        // req: 5.0/MQTT-3.2.2-9
        assert!(a.reason.unwrap_or(0) < 0x80, "QoS {qos}: {a:?}");
    }
    // req: 5.0/MQTT-3.2.2-9
    c.ping().unwrap();
}

/// The server's Maximum QoS if below 2, else records a skip.
fn reduced_maximum_qos(broker: &crate::Broker) -> Option<u8> {
    let (_, ack) = super::session_with_ack(broker);
    let max = maximum_qos(&ack);
    if max == 2 {
        crate::outcome::skipped("broker supports QoS 2 (no Maximum QoS in CONNACK)");
        None
    } else {
        Some(max)
    }
}

pub fn reduced_maximum_qos_still_accepts_every_subscription_qos() {
    let broker = broker_or_skip!();
    let Some(max) = reduced_maximum_qos(&broker) else {
        return;
    };
    let t = super::topic();
    let mut c = session(&broker);
    let filters: Vec<String> = (0..3).map(|q| format!("{t}/{q}")).collect();
    let ack = c
        .subscribe_many(
            &[(&filters[0], 0), (&filters[1], 1), (&filters[2], 2)],
            Vec::new(),
        )
        .unwrap();
    assert_eq!(ack.codes.len(), 3, "{ack:?}");
    for (requested, code) in ack.codes.iter().enumerate() {
        // req: 5.0/MQTT-3.2.2-10
        assert!(
            *code < 0x80,
            "QoS {requested} refused with Maximum QoS {max}: {ack:?}"
        );
    }
}

/// "It is a Protocol Error if the Server receives a PUBLISH packet with a QoS
/// greater than the Maximum QoS it specified" (§3.2.2.3.4): the server must
/// close (MQTT-4.13.1-1). 0x9B is the code the spec names; another is noted.
pub fn publish_above_maximum_qos_closes() {
    let broker = broker_or_skip!();
    let Some(max) = reduced_maximum_qos(&broker) else {
        return;
    };
    let mut c = session(&broker);
    let mut p = crate::Publish::new(&super::topic(), b"too-high");
    p.qos = max + 1;
    p.packet_id = Some(1);
    // On purpose above the announced Maximum QoS: bypass the harness's gate.
    c.send_raw(&Packet::Publish(p).encode(V)).unwrap();
    // req: 5.0/MQTT-3.2.2-11
    // req: 5.0/MQTT-4.13.1-1
    let reason = c.expect_error_close(default_timeout()).unwrap();
    if let Some(r) = reason.filter(|r| *r != 0x9B) {
        crate::outcome::note(format!("DISCONNECT {r:#04x}, not 0x9B (QoS not supported)"));
    }
}

pub fn will_qos_above_maximum_qos_is_refused() {
    let broker = broker_or_skip!();
    let Some(max) = reduced_maximum_qos(&broker) else {
        return;
    };
    let mut will = Will::new(&super::topic(), b"will");
    will.qos = max + 1;
    // 0x9B is recommended, any refusal is allowed.
    // req: 5.0/MQTT-3.2.2-12
    refused(&broker, &Connect::new(V, &unique_id("wqm")).will(will));
}
