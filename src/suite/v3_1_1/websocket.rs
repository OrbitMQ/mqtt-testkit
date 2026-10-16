// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! MQTT over WebSocket (section 6). Cases skip unless a WebSocket endpoint
//! is configured.

use std::time::Instant;

use crate::ws::{self, OP_BINARY, OP_TEXT, WebSocket};
use crate::{Client, Connect, Packet, Publish, Subscribe, default_timeout, unique_id};

use super::V;

/// The configured WebSocket endpoint, or `None` after recording a skip.
fn endpoint() -> Option<String> {
    ws::endpoint_or_skip()
}

/// Upgrades (offering `mqtt`), sends CONNECT and requires CONNACK 0.
fn connected(url: &str, prefix: &str) -> Client {
    let mut c = Client::open_ws(url, V).unwrap();
    let ack = c.connect(&Connect::new(V, &unique_id(prefix))).unwrap();
    assert_eq!(ack.code, 0x00, "CONNECT over WebSocket refused: {ack:?}");
    c
}

fn encode(p: &Packet) -> Vec<u8> {
    p.encode(V)
}

/// Collects packets until `done` holds, acknowledging QoS 1 `PUBLISH` packets.
fn collect_until(c: &mut Client, mut done: impl FnMut(&[Packet]) -> bool) -> Vec<Packet> {
    let deadline = Instant::now() + default_timeout();
    let mut seen = Vec::new();
    while !done(&seen) {
        let left = deadline.saturating_duration_since(Instant::now());
        let p = c
            .expect_packet(left)
            .unwrap_or_else(|e| panic!("{e} (received so far: {seen:?})"));
        if let Packet::Publish(p) = &p
            && let (1, Some(id)) = (p.qos, p.packet_id)
        {
            c.send(&Packet::PubAck(crate::Ack::new(id))).unwrap();
        }
        seen.push(p);
    }
    seen
}

/// The server answers the upgrade selecting the `mqtt` subprotocol, also
/// when the client offers another one first.
pub fn server_selects_mqtt_subprotocol() {
    let Some(url) = endpoint() else { return };
    for offered in [&["mqtt"][..], &["x-unknown-protocol", "mqtt"][..]] {
        let ws = WebSocket::connect(&url, offered)
            .unwrap_or_else(|e| panic!("offering {offered:?}: {e}"));
        // req: 3.1.1/MQTT-6.0.0-4
        assert_eq!(
            ws.handshake().subprotocols(),
            ["mqtt"],
            "offering {offered:?}, the server returned {:?}",
            ws.handshake().headers
        );
    }
}

/// CONNECT, SUBSCRIBE, PUBLISH (QoS 1) and PINGREQ over WebSocket; every
/// MQTT packet from the server arrives in binary frames.
pub fn connect_subscribe_publish_round_trip_over_websocket() {
    let Some(url) = endpoint() else { return };
    let mut c = connected(&url, "wsrt");
    let topic = format!("ws/{}", unique_id("rt"));
    let ack = c.subscribe(&topic, 1).unwrap();
    // A server may grant QoS 0 (3.8.4); the round trip works either way.
    assert!(matches!(ack.codes.as_slice(), [0 | 1]), "{ack:?}");
    c.publish(&topic, b"over websocket", 1, false).unwrap();
    let p = c.expect_publish(default_timeout()).unwrap();
    assert_eq!(
        (p.topic.as_str(), p.payload.as_slice()),
        (topic.as_str(), &b"over websocket"[..])
    );
    c.ping().unwrap();
    let stats = c.websocket().unwrap().stats();
    // The server may pack several packets into one frame.
    // req: 3.1.1/MQTT-6.0.0-1
    assert!(stats.binary >= 1 && stats.text == 0, "{stats:?}");
    c.disconnect().unwrap();
}

/// A text data frame on an established MQTT connection makes the server
/// close the connection (no PINGRESP for the PINGREQ it carried).
pub fn text_frame_after_connect_closes_connection() {
    let Some(url) = endpoint() else { return };
    let mut c = connected(&url, "wstxt");
    c.websocket()
        .unwrap()
        .send_frame(true, OP_TEXT, &encode(&Packet::PingReq))
        .unwrap();
    // No packet (in 5.0 at most an error DISCONNECT) before the close.
    // req: 3.1.1/MQTT-6.0.0-1
    c.expect_error_close(default_timeout()).unwrap();
}

/// A CONNECT sent in a text data frame is not accepted: the server closes
/// the connection without a successful CONNACK.
pub fn text_frame_carrying_connect_closes_connection() {
    let Some(url) = endpoint() else { return };
    let mut c = Client::open_ws(&url, V).unwrap();
    let connect = encode(&Packet::Connect(Box::new(Connect::new(
        V,
        &unique_id("wstc"),
    ))));
    c.websocket()
        .unwrap()
        .send_frame(true, OP_TEXT, &connect)
        .unwrap();
    // req: 3.1.1/MQTT-6.0.0-1
    let seen = c.expect_closed(default_timeout()).unwrap();
    assert!(
        !seen
            .iter()
            .any(|p| matches!(p, Packet::ConnAck(a) if a.code == 0)),
        "CONNECT in a text frame was accepted: {seen:?}"
    );
}

/// MQTT packets split over several WebSocket messages (one byte each) and
/// over the fragments of one message are reassembled.
pub fn packet_split_across_frames_is_reassembled() {
    let Some(url) = endpoint() else { return };
    let mut c = Client::open_ws(&url, V).unwrap();
    let connect = encode(&Packet::Connect(Box::new(Connect::new(
        V,
        &unique_id("wssp"),
    ))));
    c.websocket()
        .unwrap()
        .send_binary_chunks(&connect, 1)
        .unwrap();
    // req: 3.1.1/MQTT-6.0.0-2
    match c.expect_packet(default_timeout()).unwrap() {
        Packet::ConnAck(a) => assert_eq!(a.code, 0x00, "{a:?}"),
        p => panic!("expected CONNACK, got {p:?}"),
    }
    let topic = format!("ws/{}", unique_id("sp"));
    c.subscribe(&topic, 1).unwrap();
    let mut publish = Publish::new(&topic, b"fragmented payload");
    publish.qos = 1;
    publish.packet_id = Some(7);
    let bytes = encode(&Packet::Publish(publish));
    // One WebSocket message in 3-byte fragments (binary + continuations).
    c.websocket()
        .unwrap()
        .send_fragmented(OP_BINARY, &bytes, 3)
        .unwrap();
    let seen = collect_until(&mut c, |s| {
        s.iter()
            .any(|p| matches!(p, Packet::PubAck(a) if a.packet_id == 7))
            && s.iter().any(|p| matches!(p, Packet::Publish(_)))
    });
    // req: 3.1.1/MQTT-6.0.0-2
    assert!(
        seen.iter()
            .any(|p| matches!(p, Packet::Publish(p) if p.payload == b"fragmented payload")),
        "{seen:?}"
    );
    // PINGREQ split into two messages.
    c.websocket()
        .unwrap()
        .send_binary_chunks(&encode(&Packet::PingReq), 1)
        .unwrap();
    // req: 3.1.1/MQTT-6.0.0-2
    assert_eq!(
        c.expect_packet(default_timeout()).unwrap(),
        Packet::PingResp
    );
}

/// CONNECT, SUBSCRIBE, PUBLISH and PINGREQ packed into one binary frame are
/// all processed.
pub fn several_packets_in_one_frame_are_processed() {
    let Some(url) = endpoint() else { return };
    let mut c = Client::open_ws(&url, V).unwrap();
    let topic = format!("ws/{}", unique_id("mp"));
    let mut publish = Publish::new(&topic, b"packed");
    publish.qos = 1;
    publish.packet_id = Some(2);
    let mut bytes = encode(&Packet::Connect(Box::new(Connect::new(
        V,
        &unique_id("wsmp"),
    ))));
    bytes.extend(encode(&Packet::Subscribe(Subscribe {
        packet_id: 1,
        properties: Vec::new(),
        filters: vec![(topic.clone(), 1)],
    })));
    bytes.extend(encode(&Packet::Publish(publish)));
    bytes.extend(encode(&Packet::PingReq));
    c.websocket().unwrap().send_binary(&bytes).unwrap();
    // Responses to different packets may come in any order.
    let seen = collect_until(&mut c, |s| {
        s.contains(&Packet::PingResp)
            && s.iter()
                .any(|p| matches!(p, Packet::PubAck(a) if a.packet_id == 2))
            && s.iter().any(|p| matches!(p, Packet::Publish(_)))
    });
    // req: 3.1.1/MQTT-6.0.0-2
    assert!(
        matches!(seen.first(), Some(Packet::ConnAck(a)) if a.code == 0),
        "{seen:?}"
    );
    // req: 3.1.1/MQTT-6.0.0-2
    assert!(
        seen.iter()
            .any(|p| matches!(p, Packet::SubAck(a) if a.packet_id == 1))
            && seen
                .iter()
                .any(|p| matches!(p, Packet::PubAck(a) if a.packet_id == 2)),
        "{seen:?}"
    );
    assert!(
        seen.iter()
            .any(|p| matches!(p, Packet::Publish(p) if p.payload == b"packed")),
        "{seen:?}"
    );
}

/// Packets that straddle frame boundaries (each frame holds the tail of one
/// packet and the head of the next) are processed.
pub fn packets_straddling_frame_boundaries_are_processed() {
    let Some(url) = endpoint() else { return };
    let mut c = Client::open_ws(&url, V).unwrap();
    let mut bytes = encode(&Packet::Connect(Box::new(Connect::new(
        V,
        &unique_id("wsst"),
    ))));
    for _ in 0..3 {
        bytes.extend(encode(&Packet::PingReq));
    }
    c.websocket()
        .unwrap()
        .send_binary_chunks(&bytes, 5)
        .unwrap();
    let seen = collect_until(&mut c, |s| {
        s.iter().filter(|p| **p == Packet::PingResp).count() == 3
    });
    // req: 3.1.1/MQTT-6.0.0-2
    assert!(
        matches!(seen.as_slice(), [Packet::ConnAck(a), _, _, _] if a.code == 0),
        "{seen:?}"
    );
}
