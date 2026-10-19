// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! CONNECT / CONNACK validation (MQTT 3.1.1 sections 3.1, 3.2).

use crate::{Connect, Packet, Will, Writer, broker_or_skip, default_timeout, unique_id};

use super::{V, assert_connect_closed_without_connack, session};

pub fn valid_connect_is_accepted_with_connack_first() {
    let broker = broker_or_skip!();
    // `connect` fails unless the first packet from the broker is CONNACK.
    // req: 3.1.1/MQTT-3.2.0-1
    let (mut c, ack) = broker.connect(&Connect::new(V, &unique_id("acc"))).unwrap();
    // req: 3.1.1/MQTT-3.1.4-4
    assert_eq!(ack.code, 0x00);
    // req: 3.1.1/MQTT-3.2.2-1
    assert!(!ack.session_present());
    // The whole suite runs over plain TCP: an ordered, lossless,
    // bidirectional byte stream.
    // req: 3.1.1/MQTT-7.1.1-1
    c.ping().unwrap();
}

pub fn client_id_of_23_alphanumerics_is_accepted() {
    let broker = broker_or_skip!();
    let mut id = unique_id("Z9");
    while id.len() < 23 {
        id.push('a');
    }
    let (_c, ack) = broker.connect(&Connect::new(V, &id)).unwrap();
    // req: 3.1.1/MQTT-3.1.3-5
    assert_eq!(ack.code, 0x00, "23-byte client id {id} refused");
}

pub fn unsupported_protocol_level_gets_connack_0x01_then_close() {
    let broker = broker_or_skip!();
    for level in [0u8, 1, 2] {
        let mut c = broker.open(V).unwrap();
        let connect = Connect::new(V, &unique_id("lvl")).protocol("MQTT", level);
        c.send(&Packet::Connect(Box::new(connect))).unwrap();
        // req: 3.1.1/MQTT-3.2.2-5
        let ack = c.expect_refused(default_timeout()).unwrap_or_else(|e| {
            panic!("level {level}: {e} (MQTT-3.1.2-2 requires a 3.1.1 CONNACK 0x01)")
        });
        let ack = ack.unwrap_or_else(|| panic!("level {level}: closed without CONNACK 0x01"));
        // req: 3.1.1/MQTT-3.1.2-2
        assert_eq!(ack.code, 0x01, "level {level}");
        // req: 3.1.1/MQTT-3.2.2-4
        assert!(!ack.session_present(), "level {level}");
    }
}

pub fn foreign_protocol_name_is_not_processed_as_mqtt() {
    let broker = broker_or_skip!();
    for name in ["MQTX", "mqtt", "MQIsdpX"] {
        let mut c = broker.open(V).unwrap();
        let connect = Connect::new(V, &unique_id("name")).protocol(name, 4);
        c.send(&Packet::Connect(Box::new(connect))).unwrap();
        // req: 3.1.1/MQTT-3.1.2-1
        let what = c
            .expect_foreign_protocol_not_accepted()
            .unwrap_or_else(|e| panic!("protocol name {name:?}: {e}"));
        crate::outcome::note(format!("protocol name {name:?}: {what}"));
    }
}

pub fn reserved_connect_flag_closes_without_connack() {
    let broker = broker_or_skip!();
    // req: 3.1.1/MQTT-3.1.2-3
    // req: 3.1.1/MQTT-3.1.4-1
    assert_connect_closed_without_connack(&broker, &Connect::new(V, &unique_id("rsv")).flags(0x03));
}

pub fn will_qos_without_will_flag_closes() {
    let broker = broker_or_skip!();
    for flags in [0x0A, 0x12] {
        // req: 3.1.1/MQTT-3.1.2-11
        // req: 3.1.1/MQTT-3.1.2-13
        // req: 3.1.1/MQTT-3.1.4-1
        assert_connect_closed_without_connack(
            &broker,
            &Connect::new(V, &unique_id("wq")).flags(flags),
        );
    }
}

pub fn will_fields_without_will_flag_close() {
    let broker = broker_or_skip!();
    // Will Topic and Will Message in the payload, but Connect Flags only
    // carry CleanSession (0x02).
    let connect = Connect::new(V, &unique_id("wff"))
        .will(Will::new("conformance/will", b"x"))
        .flags(0x02);
    // req: 3.1.1/MQTT-3.1.2-11
    // req: 3.1.1/MQTT-3.1.4-1
    assert_connect_closed_without_connack(&broker, &connect);
}

pub fn will_retain_without_will_flag_closes() {
    let broker = broker_or_skip!();
    // req: 3.1.1/MQTT-3.1.2-15
    // req: 3.1.1/MQTT-3.1.2-11
    // req: 3.1.1/MQTT-3.1.4-1
    assert_connect_closed_without_connack(&broker, &Connect::new(V, &unique_id("wr")).flags(0x22));
}

pub fn will_qos_3_closes() {
    let broker = broker_or_skip!();
    let mut will = Will::new("conformance/will", b"x");
    will.qos = 3;
    // req: 3.1.1/MQTT-3.1.2-14
    // req: 3.1.1/MQTT-3.1.4-1
    assert_connect_closed_without_connack(&broker, &Connect::new(V, &unique_id("w3")).will(will));
}

pub fn will_flag_without_will_fields_closes() {
    let broker = broker_or_skip!();
    // req: 3.1.1/MQTT-3.1.2-9
    // req: 3.1.1/MQTT-3.1.4-1
    assert_connect_closed_without_connack(&broker, &Connect::new(V, &unique_id("wf")).flags(0x06));
}

pub fn password_without_username_closes() {
    let broker = broker_or_skip!();
    let connect = Connect::new(V, &unique_id("pw")).password(b"secret");
    assert_eq!(connect.computed_flags(), 0x42);
    // req: 3.1.1/MQTT-3.1.2-22
    // req: 3.1.1/MQTT-3.1.4-1
    assert_connect_closed_without_connack(&broker, &connect);
}

pub fn username_flag_without_username_closes() {
    let broker = broker_or_skip!();
    // req: 3.1.1/MQTT-3.1.2-19
    // req: 3.1.1/MQTT-3.1.4-1
    assert_connect_closed_without_connack(&broker, &Connect::new(V, &unique_id("uf")).flags(0x82));
}

pub fn username_present_without_flag_closes() {
    let broker = broker_or_skip!();
    let connect = Connect::new(V, &unique_id("un"))
        .username("user")
        .flags(0x02);
    // req: 3.1.1/MQTT-3.1.2-18
    // req: 3.1.1/MQTT-3.1.4-1
    assert_connect_closed_without_connack(&broker, &connect);
}

pub fn password_flag_without_password_closes() {
    let broker = broker_or_skip!();
    let connect = Connect::new(V, &unique_id("pf"))
        .username("user")
        .flags(0xC2);
    // req: 3.1.1/MQTT-3.1.2-21
    // req: 3.1.1/MQTT-3.1.4-1
    assert_connect_closed_without_connack(&broker, &connect);
}

pub fn empty_client_id_with_clean_session_gets_unique_identity() {
    let broker = broker_or_skip!();
    let (mut a, ack_a) = broker.connect(&Connect::new(V, "")).unwrap();
    if ack_a.code != 0 {
        // The server may refuse zero-length ids, but then only with 0x02 + close.
        // req: 3.1.1/MQTT-3.1.3-9
        assert_eq!(ack_a.code, 0x02);
        a.expect_closed(default_timeout()).unwrap();
        return;
    }
    let (mut b, ack_b) = broker.connect(&Connect::new(V, "")).unwrap();
    assert_eq!(ack_b.code, 0);
    // Both stay connected: the server did not assign them the same identifier
    // (which would have made the second take over the first).
    // req: 3.1.1/MQTT-3.1.3-6
    a.ping().unwrap();
    b.ping().unwrap();
}

pub fn empty_client_id_without_clean_session_is_rejected_with_0x02() {
    let broker = broker_or_skip!();
    let mut c = broker.open(V).unwrap();
    c.send(&Packet::Connect(Box::new(Connect::new(V, "").clean(false))))
        .unwrap();
    // req: 3.1.1/MQTT-3.2.2-5
    let ack = c.expect_refused(default_timeout()).unwrap();
    let ack = ack.expect("closed without CONNACK 0x02");
    // The server enforces the client-side rule that a zero-byte ClientId
    // requires CleanSession=1.
    // req: 3.1.1/MQTT-3.1.3-7
    // req: 3.1.1/MQTT-3.1.3-8
    assert_eq!(ack.code, 0x02);
    // req: 3.1.1/MQTT-3.2.2-4
    assert!(!ack.session_present());
}

fn raw_connect_with_client_id(id: &[u8]) -> Vec<u8> {
    let mut w = Writer::new();
    w.str("MQTT").u8(4).u8(0x02).u16(60).bin(id);
    w.frame(0x10)
}

pub fn ill_formed_utf8_client_id_closes() {
    let broker = broker_or_skip!();
    // Overlong-free invalid sequence, and a UTF-16 surrogate (U+D800).
    for id in [&[b'a', 0xC3, 0x28][..], &[0xED, 0xA0, 0x80][..]] {
        let mut c = broker.open(V).unwrap();
        c.send_raw(&raw_connect_with_client_id(id)).unwrap();
        // A CONNECT that does not conform is closed without CONNACK; 0x02
        // is only for a client id that "is correct UTF-8 but not allowed".
        // req: 3.1.1/MQTT-1.5.3-1
        // req: 3.1.1/MQTT-3.1.3-4
        // req: 3.1.1/MQTT-3.1.4-1
        let packets = c
            .expect_closed(default_timeout())
            .unwrap_or_else(|e| panic!("client id {id:02x?}: {e}"));
        assert!(
            packets.is_empty(),
            "client id {id:02x?} answered with {packets:?}"
        );
    }
}

pub fn null_character_in_client_id_closes() {
    let broker = broker_or_skip!();
    let mut c = broker.open(V).unwrap();
    c.send_raw(&raw_connect_with_client_id(b"ab\0cd")).unwrap();
    // req: 3.1.1/MQTT-1.5.3-2
    c.expect_refused(default_timeout()).unwrap();
}

pub fn second_connect_is_a_protocol_violation() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    c.send(&Packet::Connect(Box::new(Connect::new(
        V,
        &unique_id("dup"),
    ))))
    .unwrap();
    // req: 3.1.1/MQTT-3.1.0-2
    // req: 3.1.1/MQTT-4.8.0-1
    assert_eq!(c.expect_disconnect(default_timeout()).unwrap(), None);
}

/// PINGREQ and a PUBLISH, each sent as the first packet of a connection.
fn first_packets() -> [Packet; 2] {
    [
        Packet::PingReq,
        Packet::Publish(crate::Publish::new("conformance/first", b"x")),
    ]
}

/// The server answers a first packet other than CONNECT with nothing but a
/// refusal: it may close, refuse with a CONNACK or wait for a CONNECT.
pub fn first_packet_other_than_connect_is_not_answered() {
    let broker = broker_or_skip!();
    for packet in first_packets() {
        let mut c = broker.open(V).unwrap();
        c.send(&packet).unwrap();
        let (packets, _closed) = c.packets_until_close(default_timeout()).unwrap();
        // req: 3.1.1/MQTT-3.2.0-1
        assert!(
            matches!(
                packets.as_slice(),
                [] | [Packet::ConnAck(crate::ConnAck { code: 1..=255, .. })]
            ),
            "{} answered with {packets:?}",
            packet.name()
        );
    }
}

/// Strict: MQTT-3.1.0-1 binds the client; the only server text is a SHOULD close
/// without CONNECT (3.1.4), so closing relies on detection (MQTT-4.8.0-1).
pub fn first_packet_other_than_connect_closes() {
    let broker = broker_or_skip!();
    for packet in first_packets() {
        let mut c = broker.open(V).unwrap();
        c.send(&packet).unwrap();
        // No deadline for the close: a server may act only at its CONNECT timeout.
        // req: 3.1.1/MQTT-3.1.0-1
        c.expect_closed_eventually()
            .unwrap_or_else(|e| panic!("{}: {e}", packet.name()));
    }
}

pub fn connecting_with_a_connected_client_id_disconnects_the_old_client() {
    let broker = broker_or_skip!();
    let id = unique_id("take");
    let (mut old, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    let (mut new, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    // req: 3.1.1/MQTT-3.1.4-2
    old.expect_closed(default_timeout()).unwrap();
    new.ping().unwrap();
}

pub fn session_present_reflects_stored_session() {
    let broker = broker_or_skip!();
    let id = unique_id("sp");
    let persistent = Connect::new(V, &id).clean(false);

    let (c, ack) = broker.connect_ok(&persistent).unwrap();
    // req: 3.1.1/MQTT-3.2.2-3
    assert!(!ack.session_present(), "new persistent session");
    c.disconnect().unwrap();

    let (c, ack) = broker.connect_ok(&persistent).unwrap();
    // req: 3.1.1/MQTT-3.1.2-4
    // req: 3.1.1/MQTT-3.2.2-2
    assert!(ack.session_present(), "resumed persistent session");
    c.disconnect().unwrap();

    let (c, ack) = broker
        .connect_ok(&Connect::new(V, &id).clean(true))
        .unwrap();
    // req: 3.1.1/MQTT-3.2.2-1
    assert!(!ack.session_present(), "clean session");
    c.disconnect().unwrap();

    let (c, ack) = broker.connect_ok(&persistent).unwrap();
    // The clean session discarded the stored state.
    // req: 3.1.1/MQTT-3.1.2-6
    assert!(!ack.session_present(), "session after a clean session");
    c.disconnect().unwrap();
    // Leave no persistent session behind.
    let (c, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    c.disconnect().unwrap();
}

pub fn password_present_without_flag_closes() {
    let broker = broker_or_skip!();
    // User Name flag only (0x82), but a Password field follows the User Name.
    let connect = Connect::new(V, &unique_id("pn"))
        .username("user")
        .password(b"secret")
        .flags(0x82);
    // req: 3.1.1/MQTT-3.1.2-20
    // req: 3.1.1/MQTT-3.1.4-1
    assert_connect_closed_without_connack(&broker, &connect);
}

pub fn connect_without_client_id_field_closes() {
    let broker = broker_or_skip!();
    // Variable header only: the payload (which must start with the Client
    // Identifier) is missing altogether.
    let mut w = Writer::new();
    w.str("MQTT").u8(4).u8(0x02).u16(60);
    let mut c = broker.open(V).unwrap();
    c.send_raw(&w.frame(0x10)).unwrap();
    let packets = c.expect_closed(default_timeout()).unwrap();
    // req: 3.1.1/MQTT-3.1.3-3
    // req: 3.1.1/MQTT-3.1.4-1
    assert!(packets.is_empty(), "answered with {packets:?}");
}

pub fn ill_formed_utf8_will_topic_closes() {
    let broker = broker_or_skip!();
    for topic in [&[b'w', 0xC3, 0x28][..], &[0xED, 0xA0, 0x80][..]] {
        let mut w = Writer::new();
        w.str("MQTT")
            .u8(4)
            .u8(0x06)
            .u16(60)
            .str(&unique_id("wt"))
            .bin(topic)
            .bin(b"will");
        let mut c = broker.open(V).unwrap();
        c.send_raw(&w.frame(0x10)).unwrap();
        // No CONNACK return code applies to a bad Will Topic.
        // req: 3.1.1/MQTT-3.1.3-10
        // req: 3.1.1/MQTT-1.5.3-1
        // req: 3.1.1/MQTT-3.1.4-1
        // req: 3.1.1/MQTT-3.2.2-6
        let packets = c
            .expect_closed(default_timeout())
            .unwrap_or_else(|e| panic!("will topic {topic:02x?}: {e}"));
        assert!(
            packets.is_empty(),
            "will topic {topic:02x?} answered with {packets:?}"
        );
    }
}

pub fn ill_formed_utf8_user_name_closes() {
    let broker = broker_or_skip!();
    for name in [&[b'u', 0xFF][..], &[0xED, 0xA0, 0x80][..]] {
        let mut w = Writer::new();
        w.str("MQTT")
            .u8(4)
            .u8(0x82)
            .u16(60)
            .str(&unique_id("un"))
            .bin(name);
        let mut c = broker.open(V).unwrap();
        c.send_raw(&w.frame(0x10)).unwrap();
        // A close, or CONNACK 0x04 ("The data in the user name or password
        // is malformed") then a close.
        // req: 3.1.1/MQTT-3.1.3-11
        // req: 3.1.1/MQTT-1.5.3-1
        let ack = c
            .expect_refused(default_timeout())
            .unwrap_or_else(|e| panic!("user name {name:02x?}: {e}"));
        if let Some(ack) = ack {
            assert_eq!(ack.code, 0x04, "user name {name:02x?}: {ack:?}");
        }
    }
}

pub fn data_after_a_rejected_connect_is_not_processed() {
    let broker = broker_or_skip!();
    let t = format!("conformance/{}", unique_id("rej"));
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let rejected = [
        // Unsupported protocol level: CONNACK 0x01.
        Connect::new(V, &unique_id("rl")).protocol("MQTT", 2),
        // Zero-byte ClientId with CleanSession=0: CONNACK 0x02.
        Connect::new(V, "").clean(false),
    ];
    for connect in rejected {
        // CONNECT and a retained QoS 1 PUBLISH in one write.
        let mut bytes = Packet::Connect(Box::new(connect.clone())).encode(V);
        let mut p = crate::Publish::new(&t, b"leaked");
        p.qos = 1;
        p.retain = true;
        p.packet_id = Some(1);
        bytes.extend(Packet::Publish(p).encode(V));
        // The refusal itself is checked by other cases: skip a CONNECT this
        // broker accepts, and do not judge the shape of its refusal here.
        let mut probe = broker.open(V).unwrap();
        probe
            .send(&Packet::Connect(Box::new(connect.clone())))
            .unwrap();
        if let Ok(Packet::ConnAck(a)) = probe.expect_packet(default_timeout())
            && a.code == 0
        {
            crate::outcome::note(format!("{connect:?} accepted; not a refusal to test"));
            continue;
        }
        let mut c = broker.open(V).unwrap();
        c.send_raw(&bytes).unwrap();
        // A PUBACK (or anything but the refusal) fails `expect_refused`.
        // req: 3.1.1/MQTT-3.1.4-5
        match c.expect_refused(default_timeout()) {
            Ok(_) => {}
            Err(crate::HarnessError::Decode(e)) => {
                crate::outcome::note(format!("refusal not decodable as 3.1.1: {e}"));
            }
            Err(e) => panic!("{connect:?}: {e}"),
        }
    }
    // req: 3.1.1/MQTT-3.1.4-5
    sub.expect_no_packet(crate::quiet_period()).unwrap();
    let mut late = session(&broker);
    late.subscribe(&t, 1).unwrap();
    let retained = late.collect_publishes(crate::quiet_period()).unwrap();
    if !retained.is_empty() {
        late.publish(&t, b"", 0, true).unwrap();
    }
    // req: 3.1.1/MQTT-3.1.4-5
    assert!(
        retained.is_empty(),
        "PUBLISH after a rejected CONNECT was stored: {retained:?}"
    );
}

/// A server refusing a CONNECT for a reason that has no 3.1.1 return code
/// (1 to 5) must close silently: a CONNACK with any other code is the
/// violation. Will Topics that are not valid Topic Names (wildcards, empty)
/// are refusals no return code describes; a server may also accept them, or
/// pick a return code it deems fitting (e.g. 5, Not Authorized).
pub fn refusal_without_a_fitting_return_code_sends_no_connack() {
    let broker = broker_or_skip!();
    let mut refused = 0;
    for will_topic in ["conformance/+/will", "conformance/will/#", ""] {
        let connect = Connect::new(V, &unique_id("nofit")).will(Will::new(will_topic, b"x"));
        let mut c = broker.open(V).unwrap();
        c.send(&Packet::Connect(Box::new(connect.clone()))).unwrap();
        match c.next_event(default_timeout()).unwrap() {
            crate::Event::Packet(Packet::ConnAck(a)) if a.code == 0 => {
                // Accepted: no check failed. A clean DISCONNECT discards the will.
                crate::outcome::note(format!("will topic {will_topic:?} accepted"));
                c.disconnect().unwrap();
            }
            crate::Event::Packet(Packet::ConnAck(a)) => {
                // req: 3.1.1/MQTT-3.2.2-6
                assert!(
                    (1..=5).contains(&a.code),
                    "will topic {will_topic:?}: refused with CONNACK return code {:#04x}, \
                     which Table 3.1 does not define; with no fitting code the server must \
                     close without a CONNACK",
                    a.code
                );
                crate::outcome::note(format!(
                    "will topic {will_topic:?} refused with return code {:#04x}",
                    a.code
                ));
                c.expect_closed(default_timeout()).unwrap();
                refused += 1;
            }
            crate::Event::Closed => refused += 1,
            other => {
                panic!("will topic {will_topic:?}: expected CONNACK or a close, got {other:?}")
            }
        }
    }
    if refused == 0 {
        crate::outcome::skipped("broker accepted every CONNECT; no refusal to observe");
    }
}
