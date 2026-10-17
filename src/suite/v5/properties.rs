// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! PUBLISH property pass-through, Message Expiry, request/response and
//! problem information, reason codes and assigned client identifiers
//! (MQTT 5.0).

use std::time::Duration;

use crate::{
    Ack, Connect, Packet, PropValue, Property, Publish, broker_or_skip, default_timeout, find_prop,
    prop, quiet_period, unique_id,
};

use super::{V, forget, persistent, props_of, session, topic};

/// Publishes `p` (QoS 1) to a fresh subscriber of its topic and returns the
/// delivered copy.
fn round_trip(mut p: Publish) -> Option<Publish> {
    let broker = crate::Broker::for_test()?;
    let mut sub = session(&broker);
    sub.subscribe(&p.topic, 1).unwrap();
    let mut publisher = session(&broker);
    p.qos = 1;
    publisher.publish_packet(p).unwrap();
    Some(sub.expect_publish(default_timeout()).unwrap())
}

fn with(props: Vec<Property>) -> Publish {
    let mut p = Publish::new(&topic(), b"payload");
    p.properties = props;
    p
}

pub fn payload_format_indicator_is_forwarded_unchanged() {
    for v in [0u8, 1] {
        let Some(got) = round_trip(with(vec![Property::new(
            prop::PAYLOAD_FORMAT_INDICATOR,
            PropValue::Byte(v),
        )])) else {
            return;
        };
        // An absent indicator means 0, so dropping a 0 is still "unchanged".
        let seen = find_prop(&got.properties, prop::PAYLOAD_FORMAT_INDICATOR);
        // req: 5.0/MQTT-3.3.2-4
        assert!(
            seen == Some(&PropValue::Byte(v)) || (v == 0 && seen.is_none()),
            "sent {v}, got {seen:?}"
        );
    }
}

pub fn content_type_is_forwarded_unchanged() {
    let ct = "application/vnd.orbit+json; charset=utf-8";
    let Some(got) = round_trip(with(vec![Property::new(
        prop::CONTENT_TYPE,
        PropValue::Str(ct.into()),
    )])) else {
        return;
    };
    // req: 5.0/MQTT-3.3.2-20
    assert_eq!(
        find_prop(&got.properties, prop::CONTENT_TYPE),
        Some(&PropValue::Str(ct.into()))
    );
}

pub fn response_topic_and_correlation_data_are_forwarded_unchanged() {
    let reply = format!("{}/reply", topic());
    let corr = vec![0u8, 0xFF, 0x10, 0x00, 0x7F];
    let Some(got) = round_trip(with(vec![
        Property::new(prop::RESPONSE_TOPIC, PropValue::Str(reply.clone())),
        Property::new(prop::CORRELATION_DATA, PropValue::Bin(corr.clone())),
    ])) else {
        return;
    };
    // req: 5.0/MQTT-3.3.2-15
    assert_eq!(
        find_prop(&got.properties, prop::RESPONSE_TOPIC),
        Some(&PropValue::Str(reply))
    );
    // req: 5.0/MQTT-3.3.2-16
    assert_eq!(
        find_prop(&got.properties, prop::CORRELATION_DATA),
        Some(&PropValue::Bin(corr))
    );
}

pub fn user_properties_are_forwarded_in_order_with_duplicates() {
    let pairs = [
        ("z", "1"),
        ("a", "2"),
        ("z", "3"),
        ("", ""),
        ("k\u{e9}y", "v\u{1F680}"),
        ("a", "2"),
    ];
    let props: Vec<Property> = pairs
        .iter()
        .map(|(k, v)| {
            Property::new(
                prop::USER_PROPERTY,
                PropValue::Pair((*k).into(), (*v).into()),
            )
        })
        .collect();
    let Some(got) = round_trip(with(props)) else {
        return;
    };
    let want: Vec<PropValue> = pairs
        .iter()
        .map(|(k, v)| PropValue::Pair((*k).into(), (*v).into()))
        .collect();
    // req: 5.0/MQTT-3.3.2-17
    // req: 5.0/MQTT-3.3.2-18
    assert_eq!(props_of(&got.properties, prop::USER_PROPERTY), want);
}

pub fn properties_survive_offline_queueing() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("pq");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    let mut publisher = session(&broker);
    let mut p = Publish::new(&t, b"queued");
    p.qos = 1;
    p.properties = vec![
        Property::new(prop::CONTENT_TYPE, PropValue::Str("text/plain".into())),
        Property::new(prop::USER_PROPERTY, PropValue::Pair("b".into(), "1".into())),
        Property::new(prop::USER_PROPERTY, PropValue::Pair("a".into(), "2".into())),
    ];
    publisher.publish_packet(p).unwrap();
    let (mut c, _) = persistent(&broker, &id);
    let got = c.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.3.2-20
    assert_eq!(
        find_prop(&got.properties, prop::CONTENT_TYPE),
        Some(&PropValue::Str("text/plain".into()))
    );
    // req: 5.0/MQTT-3.3.2-18
    assert_eq!(
        props_of(&got.properties, prop::USER_PROPERTY),
        [
            PropValue::Pair("b".into(), "1".into()),
            PropValue::Pair("a".into(), "2".into())
        ]
    );
    c.disconnect().unwrap();
    forget(&broker, &id);
}

fn expiry_of(p: &Publish) -> Option<u32> {
    match find_prop(&p.properties, prop::MESSAGE_EXPIRY_INTERVAL) {
        None => None,
        Some(PropValue::FourByte(v)) => Some(*v),
        Some(other) => panic!("Message Expiry Interval of type {other:?}"),
    }
}

fn expiring(topic: &str, payload: &[u8], secs: u32) -> Publish {
    let mut p = Publish::new(topic, payload);
    p.qos = 1;
    p.properties = vec![Property::new(
        prop::MESSAGE_EXPIRY_INTERVAL,
        PropValue::FourByte(secs),
    )];
    p
}

pub fn live_delivery_carries_the_message_expiry_interval() {
    let Some(got) = round_trip(expiring(&topic(), b"e", 120)) else {
        return;
    };
    let left = expiry_of(&got);
    // req: 5.0/MQTT-3.3.2-6
    assert!(left.is_some_and(|s| (118..=120).contains(&s)), "{got:?}");
}

pub fn queued_message_expiry_is_reduced_by_the_waiting_time() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("me");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    let mut publisher = session(&broker);
    publisher
        .publish_packet(expiring(&t, b"waited", 60))
        .unwrap();
    std::thread::sleep(Duration::from_millis(2_200));
    let (mut c, _) = persistent(&broker, &id);
    let got = c.expect_publish(default_timeout()).unwrap();
    let left = expiry_of(&got);
    // req: 5.0/MQTT-3.3.2-6
    assert!(left.is_some_and(|s| (1..=58).contains(&s)), "{got:?}");
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn expired_queued_message_is_not_delivered() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = unique_id("mx");
    let (mut c, _) = persistent(&broker, &id);
    c.subscribe(&t, 1).unwrap();
    c.disconnect().unwrap();
    let mut publisher = session(&broker);
    publisher.publish_packet(expiring(&t, b"stale", 1)).unwrap();
    publisher.publish(&t, b"fresh", 1, false).unwrap();
    std::thread::sleep(Duration::from_millis(2_200));
    let (mut c, _) = persistent(&broker, &id);
    let got = c.collect_publishes(quiet_period()).unwrap();
    // req: 5.0/MQTT-3.3.2-5
    assert_eq!(
        got.iter().map(|p| p.payload.clone()).collect::<Vec<_>>(),
        [b"fresh".to_vec()]
    );
    c.disconnect().unwrap();
    forget(&broker, &id);
}

pub fn expired_retained_message_is_not_delivered() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    let mut p = expiring(&t, b"short-lived", 1);
    p.retain = true;
    publisher.publish_packet(p).unwrap();
    std::thread::sleep(Duration::from_millis(2_200));
    let mut late = session(&broker);
    late.subscribe(&t, 1).unwrap();
    // req: 5.0/MQTT-3.3.2-5
    late.expect_no_packet(quiet_period()).unwrap();
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn retained_message_expiry_is_reduced_by_the_waiting_time() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut publisher = session(&broker);
    let mut p = expiring(&t, b"aging", 60);
    p.retain = true;
    publisher.publish_packet(p).unwrap();
    std::thread::sleep(Duration::from_millis(2_200));
    let mut late = session(&broker);
    late.subscribe(&t, 1).unwrap();
    let got = late.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.3.2-6
    assert!(
        expiry_of(&got).is_some_and(|s| (1..=58).contains(&s)),
        "{got:?}"
    );
    publisher.publish(&t, b"", 0, true).unwrap();
}

pub fn response_information_is_omitted_unless_requested() {
    let broker = broker_or_skip!();
    for request in [None, Some(0u8)] {
        let mut connect = Connect::new(V, &unique_id("ri"));
        if let Some(r) = request {
            connect = connect.property(prop::REQUEST_RESPONSE_INFORMATION, PropValue::Byte(r));
        }
        let (_c, ack) = broker.connect_ok(&connect).unwrap();
        // req: 5.0/MQTT-3.1.2-28
        assert_eq!(
            find_prop(&ack.properties, prop::RESPONSE_INFORMATION),
            None,
            "{ack:?}"
        );
    }
    // Requested: the server may (but need not) return it.
    let (_c, ack) = broker
        .connect_ok(
            &Connect::new(V, &unique_id("ri1"))
                .property(prop::REQUEST_RESPONSE_INFORMATION, PropValue::Byte(1)),
        )
        .unwrap();
    if let Some(v) = find_prop(&ack.properties, prop::RESPONSE_INFORMATION) {
        assert!(matches!(v, PropValue::Str(_)), "{v:?}");
    }
}

pub fn request_problem_information_0_suppresses_reason_strings() {
    let broker = broker_or_skip!();
    let (mut c, _) = broker
        .connect_ok(
            &Connect::new(V, &unique_id("rp"))
                .property(prop::REQUEST_PROBLEM_INFORMATION, PropValue::Byte(0)),
        )
        .unwrap();
    let t = topic();
    let check = |props: &[Property], what: &str| {
        // req: 5.0/MQTT-3.1.2-29
        assert!(
            props
                .iter()
                .all(|p| p.id != prop::REASON_STRING && p.id != prop::USER_PROPERTY),
            "{what} carries problem information: {props:?}"
        );
    };
    let ack = c
        .subscribe_many(&[(&t, 1), (&format!("{t}/b"), 0)], Vec::new())
        .unwrap();
    check(&ack.properties, "SUBACK");
    let ack = c.unsubscribe(&format!("{t}/none")).unwrap();
    check(&ack.properties, "UNSUBACK");
    // QoS 1 with no subscriber may get PUBACK 0x10 (No matching subscribers).
    let mut p = Publish::new(&format!("{t}/nobody"), b"x");
    p.qos = 1;
    let ack = c.publish_packet(p).unwrap().unwrap();
    check(&ack.properties, "PUBACK");
    let mut p = Publish::new(&format!("{t}/nobody"), b"x");
    p.qos = 2;
    let rec = c.publish_packet(p).unwrap().unwrap();
    check(&rec.properties, "PUBREC");
    // PUBCOMP, driven by hand (publish_packet consumes it).
    let mut flow = Publish::new(&format!("{t}/nobody"), b"x");
    flow.qos = 2;
    flow.packet_id = Some(77);
    c.send(&Packet::Publish(flow)).unwrap();
    let rec = c
        .wait_for(default_timeout(), "PUBREC", |p| match p {
            Packet::PubRec(a) if a.packet_id == 77 => Some(a.clone()),
            _ => None,
        })
        .unwrap();
    if rec.reason.unwrap_or(0) < 0x80 {
        c.send(&Packet::PubRel(crate::Ack::new(77))).unwrap();
        let comp = c
            .wait_for(default_timeout(), "PUBCOMP", |p| match p {
                Packet::PubComp(a) if a.packet_id == 77 => Some(a.clone()),
                _ => None,
            })
            .unwrap();
        check(&comp.properties, "PUBCOMP");
    }
    // PUBCOMP for an unknown identifier (0x92).
    c.send(&Packet::PubRel(crate::Ack::new(999))).unwrap();
    let comp = c
        .wait_for(default_timeout(), "PUBCOMP", |p| match p {
            Packet::PubComp(a) if a.packet_id == 999 => Some(a.clone()),
            _ => None,
        })
        .unwrap();
    check(&comp.properties, "PUBCOMP for an unknown identifier");
    // The server's PUBREL, on a QoS 2 delivery to this client.
    let q2 = format!("{t}/q2");
    c.subscribe_needing(&q2, 2);
    let mut publisher = super::session(&broker);
    publisher.publish(&q2, b"x", 2, false).unwrap();
    let delivery = c.expect_publish_unacked(default_timeout()).unwrap();
    let id = delivery.packet_id.unwrap();
    c.send(&Packet::PubRec(crate::Ack::new(id))).unwrap();
    let rel = c
        .wait_for(default_timeout(), "PUBREL", |p| match p {
            Packet::PubRel(a) if a.packet_id == id => Some(a.clone()),
            _ => None,
        })
        .unwrap();
    check(&rel.properties, "PUBREL");
    c.send(&Packet::PubComp(crate::Ack::new(id))).unwrap();
}

pub fn acknowledgement_reason_codes_are_valid() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut c = session(&broker);
    // The PUBACK and PUBREC Reason Code tables (3.4.2.1, 3.5.2.1) are the
    // same; nobody subscribed, so 0x00 or 0x10 is expected, but any defined
    // code is valid.
    let valid = |code: u8| {
        matches!(
            code,
            0x00 | 0x10 | 0x80 | 0x83 | 0x87 | 0x90 | 0x91 | 0x97 | 0x99
        )
    };
    let mut p = Publish::new(&t, b"x");
    p.qos = 1;
    let ack = c.publish_packet(p).unwrap().unwrap();
    // req: 5.0/MQTT-3.4.2-1
    assert!(valid(ack.reason.unwrap_or(0)), "{ack:?}");
    let mut p = Publish::new(&t, b"x");
    p.qos = 2;
    let rec = c.publish_packet(p).unwrap().unwrap();
    // req: 5.0/MQTT-3.5.2-1
    assert!(valid(rec.reason.unwrap_or(0)), "{rec:?}");
}

pub fn pubrel_for_an_unknown_identifier_gets_pubcomp() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    c.send(&Packet::PubRel(Ack::new(4242))).unwrap();
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::PubComp(a) => {
            assert_eq!(a.packet_id, 4242);
            // 0x92 (Packet Identifier not found) is the precise answer; 0x00
            // is also a valid PUBCOMP code.
            // req: 5.0/MQTT-3.7.2-1
            assert!(matches!(a.reason.unwrap_or(0), 0x00 | 0x92), "{a:?}");
        }
        other => panic!("expected PUBCOMP, got {other:?}"),
    }
}

pub fn server_pubrel_reason_code_is_valid() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe_needing(&t, 2);
    let mut publisher = session(&broker);
    publisher.publish(&t, b"x", 2, false).unwrap();
    let p = sub.expect_publish_unacked(default_timeout()).unwrap();
    let id = p.packet_id.unwrap();
    sub.send(&Packet::PubRec(Ack::new(id))).unwrap();
    match sub.expect_packet(default_timeout()).unwrap() {
        Packet::PubRel(a) => {
            // req: 5.0/MQTT-3.6.2-1
            assert!(matches!(a.reason.unwrap_or(0), 0x00 | 0x92), "{a:?}");
        }
        other => panic!("expected PUBREL, got {other:?}"),
    }
    sub.send(&Packet::PubComp(Ack::new(id))).unwrap();
}

pub fn unsuback_has_one_valid_reason_code_per_filter_in_order() {
    let broker = broker_or_skip!();
    let t = topic();
    let (a, b, c_) = (format!("{t}/a"), format!("{t}/b"), format!("{t}/c"));
    let mut c = session(&broker);
    c.subscribe(&a, 0).unwrap();
    c.subscribe(&c_, 0).unwrap();
    let ack = c.unsubscribe_many(&[&a, &b, &c_]).unwrap();
    // req: 5.0/MQTT-3.11.3-1
    // req: 5.0/MQTT-3.11.3-2
    assert_eq!(ack.codes.len(), 3, "{ack:?}");
    assert_eq!(ack.codes[0], 0x00, "{ack:?}");
    // 0x11 (No subscription existed) for b; 0x00 is tolerated.
    assert!(matches!(ack.codes[1], 0x00 | 0x11), "{ack:?}");
    assert_eq!(ack.codes[2], 0x00, "{ack:?}");
}

/// The Subscribe Reason Codes of section 3.9.3.
const VALID_SUBACK_CODES: [u8; 12] = [
    0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2,
];

pub fn suback_reason_codes_are_valid() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut c = session(&broker);
    let ack = c
        .subscribe_many(
            &[
                (&format!("{t}/0"), 0),
                (&format!("{t}/1"), 1),
                (&format!("{t}/2"), 2),
            ],
            Vec::new(),
        )
        .unwrap();
    // req: 5.0/MQTT-3.9.3-2
    assert!(
        ack.codes.iter().all(|c| VALID_SUBACK_CODES.contains(c)),
        "{ack:?}"
    );
    // req: 5.0/MQTT-3.8.4-7
    for (i, code) in ack.codes.iter().enumerate() {
        assert!(usize::from(*code) <= i || *code >= 0x80, "{ack:?}");
    }
}

pub fn assigned_client_identifiers_are_unique_and_resume_the_session() {
    let broker = broker_or_skip!();
    let assigned =
        |ack: &crate::ConnAck| match find_prop(&ack.properties, prop::ASSIGNED_CLIENT_IDENTIFIER) {
            Some(PropValue::Str(s)) => s.clone(),
            other => panic!("no Assigned Client Identifier: {other:?}"),
        };
    let expiry = PropValue::FourByte(super::SESSION_EXPIRY);
    let mut c1 = broker.open(V).unwrap();
    c1.send(&Packet::Connect(Box::new(
        Connect::new(V, "").property(prop::SESSION_EXPIRY_INTERVAL, expiry.clone()),
    )))
    .unwrap();
    // MQTT-3.1.3-8 lets a server refuse an empty client identifier (0x85
    // and close); then there is nothing to check.
    let ack1 = match c1.next_event(default_timeout()).unwrap() {
        crate::Event::Packet(Packet::ConnAck(a)) if a.code == 0 => a,
        crate::Event::Packet(Packet::ConnAck(a)) if a.code >= 0x80 => {
            crate::outcome::skipped(format!(
                "broker refuses an empty client identifier ({:#04x})",
                a.code
            ));
            return;
        }
        crate::Event::Closed => {
            crate::outcome::skipped("broker refuses an empty client identifier (close)");
            return;
        }
        other => panic!("expected CONNACK, got {other:?}"),
    };
    let (c2, ack2) = broker.connect_ok(&Connect::new(V, "")).unwrap();
    let (id1, id2) = (assigned(&ack1), assigned(&ack2));
    // req: 5.0/MQTT-3.2.2-16
    // req: 5.0/MQTT-3.1.3-6
    assert_ne!(id1, id2);
    c2.disconnect().unwrap();
    c1.disconnect().unwrap();
    // The server treats the CONNECT as if the client had supplied the id, so
    // the session can be resumed with it.
    let (c, present) = persistent(&broker, &id1);
    // req: 5.0/MQTT-3.1.3-7
    assert!(present, "session of assigned id {id1:?} not found");
    c.disconnect().unwrap();
    forget(&broker, &id1);
}

pub fn takeover_disconnect_carries_no_session_expiry_interval() {
    let broker = broker_or_skip!();
    let id = unique_id("tk");
    let (mut old, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    let (new, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    let packets = old.expect_closed(default_timeout()).unwrap();
    for p in &packets {
        if let Packet::Disconnect(d) = p {
            // req: 5.0/MQTT-3.14.2-2
            assert_eq!(
                find_prop(&d.properties, prop::SESSION_EXPIRY_INTERVAL),
                None,
                "{d:?}"
            );
            // req: 5.0/MQTT-3.1.4-3
            assert_eq!(d.reason, Some(0x8E), "{d:?}");
        }
    }
    new.disconnect().unwrap();
}

pub fn property_length_is_present_when_there_are_no_properties() {
    use super::{Raw, frame_body, skip_properties};
    let broker = broker_or_skip!();
    let t = topic();
    let mut c = Raw::connect(&broker, &Connect::new(V, &unique_id("pl")));
    let (ack, bytes) = c.frame();
    assert!(
        matches!(ack, Packet::ConnAck(ref a) if a.code == 0),
        "{ack:?}"
    );
    let (_, body) = frame_body(&bytes);
    // Flags, Reason Code, then the properties: nothing may follow them.
    // req: 5.0/MQTT-2.2.2-1
    assert!(
        skip_properties(&body[2..]).is_empty(),
        "CONNACK {bytes:02x?}"
    );

    c.send(&Packet::Subscribe(crate::Subscribe {
        packet_id: 1,
        properties: Vec::new(),
        filters: vec![(t.clone(), 1)],
    }));
    let (ack, bytes) = c.frame();
    assert!(matches!(ack, Packet::SubAck(_)), "{ack:?}");
    // Packet Identifier, Property Length (+ properties), one reason code.
    // req: 5.0/MQTT-2.2.2-1
    assert_eq!(
        skip_properties(&frame_body(&bytes).1[2..]).len(),
        1,
        "SUBACK {bytes:02x?}"
    );

    // A QoS 0 message without properties, delivered back to this client.
    let mut publisher = session(&broker);
    let payload = b"\x00\x01\x02no-id";
    publisher.publish(&t, payload, 0, false).unwrap();
    let (p, bytes) = c.frame();
    assert!(matches!(p, Packet::Publish(_)), "{p:?}");
    let (first, body) = frame_body(&bytes);
    assert_eq!(first & 0x06, 0, "QoS 0 expected: {bytes:02x?}");
    let topic_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
    assert_eq!(&body[2..2 + topic_len], t.as_bytes());
    // Topic Name, then directly the Property Length (no Packet Identifier),
    // then the payload.
    // req: 5.0/MQTT-2.2.1-2
    // req: 5.0/MQTT-2.2.2-1
    assert_eq!(
        skip_properties(&body[2 + topic_len..]),
        payload,
        "PUBLISH {bytes:02x?}"
    );

    c.send(&Packet::Unsubscribe(crate::Unsubscribe {
        packet_id: 2,
        properties: Vec::new(),
        filters: vec![t],
    }));
    let (ack, bytes) = c.frame();
    assert!(matches!(ack, Packet::UnsubAck(_)), "{ack:?}");
    // req: 5.0/MQTT-2.2.2-1
    assert_eq!(
        skip_properties(&frame_body(&bytes).1[2..]).len(),
        1,
        "UNSUBACK {bytes:02x?}"
    );

    let mut p = Publish::new(&format!("{}/nobody", topic()), b"x");
    p.qos = 1;
    p.packet_id = Some(3);
    c.send(&Packet::Publish(p));
    let (ack, bytes) = c.frame();
    assert!(matches!(ack, Packet::PubAck(_)), "{ack:?}");
    let body = frame_body(&bytes).1;
    // Packet Identifier and Reason Code may stand alone; anything more must
    // start with a Property Length.
    if body.len() > 3 {
        // req: 5.0/MQTT-2.2.2-1
        assert!(
            skip_properties(&body[3..]).is_empty(),
            "PUBACK {bytes:02x?}"
        );
    }
}

/// The value of a Variable Byte Integer at the start of `bytes`, checking
/// that it uses the minimum number of bytes.
fn minimal_varint(bytes: &[u8], what: &str) -> u32 {
    // `decode_varint` itself rejects a multi-byte encoding ending in 0x00,
    // which is exactly a non-minimal one; the length check is explicit.
    let (v, n) = crate::codec::decode_varint(bytes)
        .unwrap_or_else(|e| panic!("{what}: {e}"))
        .unwrap_or_else(|| panic!("{what}: truncated"));
    let minimal = match v {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    };
    // req: 5.0/MQTT-1.5.5-1
    assert_eq!(n, minimal, "{what}: {v} encoded in {n} bytes: {bytes:02x?}");
    v
}

pub fn server_variable_byte_integers_are_minimal() {
    use super::Raw;
    let broker = broker_or_skip!();
    let t = topic();
    let mut c = Raw::connect(&broker, &Connect::new(V, &unique_id("vbi")));
    let (ack, bytes) = c.frame();
    let Packet::ConnAck(ack) = ack else {
        panic!("expected CONNACK, got {ack:?}")
    };
    assert_eq!(ack.code, 0, "{ack:?}");
    minimal_varint(&bytes[1..], "CONNACK Remaining Length");
    // A Subscription Identifier above 127 needs two bytes, if supported.
    let sub_ids = super::connack_u32(&ack, prop::SUBSCRIPTION_IDENTIFIER_AVAILABLE, 1) == 1;
    let properties = if sub_ids {
        vec![Property::new(
            prop::SUBSCRIPTION_IDENTIFIER,
            PropValue::VarInt(200),
        )]
    } else {
        Vec::new()
    };
    c.send(&Packet::Subscribe(crate::Subscribe {
        packet_id: 1,
        properties,
        filters: vec![(t.clone(), 0)],
    }));
    let (_, bytes) = c.frame();
    minimal_varint(&bytes[1..], "SUBACK Remaining Length");
    // A message whose properties (> 127 bytes) and whole packet (> 300
    // bytes) both need multi-byte lengths.
    let mut p = Publish::new(&t, &[b'p'; 300]);
    p.properties = vec![Property::new(
        prop::USER_PROPERTY,
        PropValue::Pair("k".into(), "v".repeat(150)),
    )];
    session(&broker).publish_packet(p).unwrap();
    let (p, bytes) = c.frame();
    let Packet::Publish(p) = p else {
        panic!("expected PUBLISH, got {p:?}")
    };
    let len = minimal_varint(&bytes[1..], "PUBLISH Remaining Length");
    assert!(len > 300, "{len}");
    let (_, body) = super::frame_body(&bytes);
    let topic_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
    let props_len = minimal_varint(&body[2 + topic_len..], "PUBLISH Property Length");
    assert!(props_len > 127, "{props_len}");
    if sub_ids {
        // Decoding already required a minimal Subscription Identifier.
        // req: 5.0/MQTT-1.5.5-1
        assert_eq!(
            props_of(&p.properties, prop::SUBSCRIPTION_IDENTIFIER),
            [PropValue::VarInt(200)]
        );
    }
}
