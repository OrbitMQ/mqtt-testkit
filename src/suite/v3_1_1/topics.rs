// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Topic names, topic filters, wildcards and `$` topics (MQTT 3.1.1).

use crate::{Broker, Packet, Subscribe, Writer, broker_or_skip, default_timeout, quiet_period};

use super::{assert_violation_closes, session, topic};

/// Subscribes to `filter`, publishes to each of `topics` and returns the
/// topics that were delivered.
fn matched(broker: &Broker, filter: &str, topics: &[String]) -> Vec<String> {
    let mut sub = session(broker);
    sub.subscribe(filter, 0).unwrap();
    let mut publisher = session(broker);
    for t in topics {
        publisher.publish(t, b"m", 0, false).unwrap();
    }
    let mut got: Vec<String> = sub
        .collect_publishes(quiet_period())
        .unwrap()
        .into_iter()
        .map(|p| p.topic)
        .collect();
    got.sort();
    got
}

pub fn plus_matches_exactly_one_level() {
    let broker = broker_or_skip!();
    let t = topic();
    let topics = [
        format!("{t}/a"),
        format!("{t}/a/b"),
        t.clone(),
        format!("{t}/"),
    ];
    let got = matched(&broker, &format!("{t}/+"), &topics);
    // "t/" has an empty last level, which "+" matches.
    // req: 3.1.1/MQTT-3.3.2-3
    // req: 3.1.1/MQTT-4.5.0-1
    assert_eq!(got, [format!("{t}/"), format!("{t}/a")]);
}

pub fn plus_in_the_middle_matches_one_level() {
    let broker = broker_or_skip!();
    let t = topic();
    let topics = [
        format!("{t}/a/x"),
        format!("{t}/b/x"),
        format!("{t}/a/y"),
        format!("{t}/a/b/x"),
    ];
    let got = matched(&broker, &format!("{t}/+/x"), &topics);
    // req: 3.1.1/MQTT-3.3.2-3
    // req: 3.1.1/MQTT-4.5.0-1
    assert_eq!(got, [format!("{t}/a/x"), format!("{t}/b/x")]);
}

pub fn hash_matches_the_parent_and_all_descendants() {
    let broker = broker_or_skip!();
    let t = topic();
    let topics = [
        t.clone(),
        format!("{t}/a"),
        format!("{t}/a/b/c"),
        format!("{t}x"),
    ];
    let got = matched(&broker, &format!("{t}/#"), &topics);
    // req: 3.1.1/MQTT-3.3.2-3
    // req: 3.1.1/MQTT-4.5.0-1
    assert_eq!(got, [t.clone(), format!("{t}/a"), format!("{t}/a/b/c")]);
}

pub fn matching_is_case_sensitive_and_does_not_normalise() {
    let broker = broker_or_skip!();
    let t = topic();
    let exact = format!("{t}/Caf\u{e9} \u{1F680}");
    let topics = [
        exact.clone(),
        format!("{t}/caf\u{e9} \u{1F680}"),
        // Decomposed e + combining acute accent: a different byte sequence.
        format!("{t}/Cafe\u{301} \u{1F680}"),
        format!("{t}/Caf\u{e9}  \u{1F680}"),
    ];
    let got = matched(&broker, &exact, &topics);
    // req: 3.1.1/MQTT-4.7.3-4
    assert_eq!(got, [exact]);
}

pub fn leading_slash_is_an_empty_level() {
    let broker = broker_or_skip!();
    let id = topic().replace("conformance/", "");
    let t = format!("/{id}");
    // With a shared broker other tests' topics may match too, so only the
    // presence or absence of `t` is checked.
    for (filter, expected) in [("+/+", true), ("/+", true), ("+", false)] {
        let got = matched(&broker, filter, std::slice::from_ref(&t));
        // req: 3.1.1/MQTT-3.3.2-3
        // req: 3.1.1/MQTT-4.5.0-1
        assert_eq!(got.contains(&t), expected, "{filter:?} vs {t:?}: {got:?}");
    }
}

pub fn wildcards_do_not_match_dollar_topics() {
    let broker = broker_or_skip!();
    let id = topic().replace("conformance/", "");
    let dollar = format!("$conformance/{id}");
    let mut exact = session(&broker);
    exact.subscribe(&dollar, 0).unwrap();
    let mut hash = session(&broker);
    hash.subscribe("#", 0).unwrap();
    let mut plus = session(&broker);
    plus.subscribe(&format!("+/{id}"), 0).unwrap();
    let mut publisher = session(&broker);
    // A server may refuse client PUBLISHes to `$` topics (it may close the
    // connection); then there is nothing to leak.
    if publisher.publish(&dollar, b"$", 1, false).is_err() {
        crate::outcome::skipped(format!("broker refuses PUBLISH to {dollar}"));
        return;
    }
    let reached_exact = exact.collect_publishes(quiet_period()).unwrap();
    if reached_exact.is_empty() {
        // Without the exact subscriber receiving it, the absence of the
        // message at the wildcard subscribers proves nothing.
        crate::outcome::skipped(format!(
            "broker did not route the client PUBLISH to {dollar}"
        ));
        return;
    }
    for (name, c) in [("#", &mut hash), ("+/id", &mut plus)] {
        let got = c.collect_publishes(quiet_period()).unwrap();
        // req: 3.1.1/MQTT-4.7.2-1
        assert!(
            got.iter().all(|p| !p.topic.starts_with('$')),
            "{name} matched {got:?}"
        );
    }
}

/// Strict: MQTT-4.7.1-2/3 are syntax rules with no server reaction; rejecting relies on detection.
pub fn invalid_topic_filters_are_rejected() {
    let broker = broker_or_skip!();
    let t = topic();
    for bad in [
        format!("{t}/#/x"),
        format!("{t}#"),
        format!("{t}/a+"),
        format!("{t}/+a/x"),
    ] {
        let mut c = session(&broker);
        let id = c.next_packet_id();
        c.send(&Packet::Subscribe(Subscribe {
            packet_id: id,
            properties: Vec::new(),
            filters: vec![(bad.clone(), 0)],
        }))
        .unwrap();
        // Allowed: SUBACK with failure code 0x80, or closing the connection.
        match c.next_event_or_close(default_timeout()).unwrap() {
            crate::Event::Packet(Packet::SubAck(s)) => {
                // req: 3.1.1/MQTT-4.7.1-2
                // req: 3.1.1/MQTT-4.7.1-3
                assert_eq!(s.codes, [0x80], "filter {bad:?}");
            }
            crate::Event::Closed => {}
            other => panic!("filter {bad:?}: {other:?}"),
        }
    }
}

/// Strict: MQTT-4.7.3-1 names no server reaction; rejecting relies on detection.
pub fn empty_topic_filter_is_rejected() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    let id = c.next_packet_id();
    c.send(&Packet::Subscribe(Subscribe {
        packet_id: id,
        properties: Vec::new(),
        filters: vec![(String::new(), 0)],
    }))
    .unwrap();
    match c.next_event_or_close(default_timeout()).unwrap() {
        crate::Event::Packet(Packet::SubAck(s)) => {
            // req: 3.1.1/MQTT-4.7.3-1
            assert_eq!(s.codes, [0x80]);
        }
        crate::Event::Closed => {}
        other => panic!("empty filter: {other:?}"),
    }
}

/// Strict: MQTT-4.7.3-1 names no server reaction; closing relies on detection (MQTT-4.8.0-1).
pub fn empty_topic_name_closes() {
    let broker = broker_or_skip!();
    let mut w = Writer::new();
    w.str("").raw(b"x");
    // req: 3.1.1/MQTT-4.7.3-1
    assert_violation_closes(&broker, &w.frame(0x30));
}

pub fn null_character_in_topic_name_closes() {
    let broker = broker_or_skip!();
    let mut w = Writer::new();
    w.str("conformance/a\0b").raw(b"x");
    // req: 3.1.1/MQTT-4.7.3-2
    // req: 3.1.1/MQTT-1.5.3-2
    assert_violation_closes(&broker, &w.frame(0x30));
}

pub fn null_character_in_topic_filter_closes() {
    let broker = broker_or_skip!();
    let mut w = Writer::new();
    w.u16(1).str("conformance/a\0b").u8(0);
    // req: 3.1.1/MQTT-4.7.3-2
    // req: 3.1.1/MQTT-1.5.3-2
    assert_violation_closes(&broker, &w.frame(0x82));
}

pub fn forwarded_topic_name_is_the_published_name() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&format!("{t}/+/z"), 0).unwrap();
    let mut publisher = session(&broker);
    publisher
        .publish(&format!("{t}/y/z"), b"n", 0, false)
        .unwrap();
    // req: 3.1.1/MQTT-3.3.2-3
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().topic,
        format!("{t}/y/z")
    );
}

pub fn bom_code_point_is_neither_skipped_nor_stripped() {
    let broker = broker_or_skip!();
    let t = topic();
    // U+FEFF encodes as EF BB BF; a server that dropped it would merge each
    // pair of topics below.
    let bom = [format!("\u{FEFF}{t}/a"), format!("{t}/\u{FEFF}b")];
    let plain = [format!("{t}/a"), format!("{t}/b")];
    let mut with_bom = session(&broker);
    let mut without = session(&broker);
    for (b, p) in bom.iter().zip(&plain) {
        with_bom.subscribe(b, 0).unwrap();
        without.subscribe(p, 0).unwrap();
    }
    let mut publisher = session(&broker);
    for name in bom.iter().chain(&plain) {
        publisher.publish(name, name.as_bytes(), 0, false).unwrap();
    }
    let topics = |c: &mut crate::Client| {
        let mut got: Vec<(String, Vec<u8>)> = c
            .collect_publishes(quiet_period())
            .unwrap()
            .into_iter()
            .map(|p| (p.topic, p.payload))
            .collect();
        got.sort();
        got
    };
    let expect = |names: &[String]| {
        let mut v: Vec<(String, Vec<u8>)> = names
            .iter()
            .map(|n| (n.clone(), n.as_bytes().to_vec()))
            .collect();
        v.sort();
        v
    };
    // req: 3.1.1/MQTT-1.5.3-3
    assert_eq!(topics(&mut with_bom), expect(&bom));
    // req: 3.1.1/MQTT-1.5.3-3
    assert_eq!(topics(&mut without), expect(&plain));
}

pub fn accepted_wildcard_filters_work_or_are_rejected() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut supported = 0;
    for (filter, name) in [
        (format!("{t}/+"), format!("{t}/x")),
        (format!("{t}/#"), format!("{t}/x/y")),
    ] {
        let mut c = session(&broker);
        let id = c.next_packet_id();
        c.send(&Packet::Subscribe(Subscribe {
            packet_id: id,
            properties: Vec::new(),
            filters: vec![(filter.clone(), 0)],
        }))
        .unwrap();
        match c.next_event_or_close(default_timeout()).unwrap() {
            crate::Event::Packet(Packet::SubAck(s)) if s.codes == [0x80] => {
                // Rejected: what a server without wildcard support must do.
                // req: 3.1.1/MQTT-3.8.3-2
                crate::outcome::note(format!("wildcard filter {filter:?} rejected"));
            }
            crate::Event::Closed => {
                crate::outcome::note(format!("wildcard filter {filter:?} closed the connection"));
            }
            crate::Event::Packet(Packet::SubAck(s)) if s.codes.len() == 1 && s.codes[0] <= 2 => {
                let mut publisher = session(&broker);
                publisher.publish(&name, b"w", 0, false).unwrap();
                // A wildcard filter the server grants must actually match.
                // req: 3.1.1/MQTT-4.5.0-1
                assert_eq!(
                    c.expect_publish(default_timeout()).unwrap().topic,
                    name,
                    "granted {filter:?} did not match"
                );
                supported += 1;
            }
            other => panic!("{filter:?}: {other:?}"),
        }
    }
    if supported == 2 {
        crate::outcome::skipped("broker supports wildcard topic filters");
    }
}
