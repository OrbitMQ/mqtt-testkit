// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Topic names, topic filters, wildcards and `$` topics (MQTT 3.1).

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
    // req: 3.1/V31-A-4
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
    // req: 3.1/V31-A-4
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
    // req: 3.1/V31-A-2
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
    // req: 3.1/V31-A-7
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
        // req: 3.1/V31-A-7
        assert_eq!(got.contains(&t), expected, "{filter:?} vs {t:?}: {got:?}");
    }
}

// No `$`-topic test for 3.1: V31-A-9 is a MAY (both behaviours conform).

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
        // 3.1 defines no SUBACK failure code and does not say how a server
        // reacts to an invalid filter. Allowed: closing the connection, or a
        // "normal-looking" SUBACK (as for unauthorised subscriptions,
        // V31-3.8-7) that must not carry the 3.1.1 code 0x80.
        match c.next_event(default_timeout()).unwrap() {
            crate::Event::Packet(Packet::SubAck(s)) => {
                // req: 3.1/V31-3.9-3
                assert!(
                    s.codes.len() == 1 && s.codes[0] <= 2,
                    "filter {bad:?}: {s:?}"
                );
            }
            crate::Event::Closed => {}
            other => panic!("filter {bad:?}: {other:?}"),
        }
    }
}

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
    match c.next_event(default_timeout()).unwrap() {
        crate::Event::Packet(Packet::SubAck(s)) => {
            // req: 3.1/V31-3.9-3
            // 3.1: no failure code exists (see `invalid_topic_filters_are_rejected`).
            assert!(s.codes.len() == 1 && s.codes[0] <= 2, "{s:?}");
        }
        crate::Event::Closed => {}
        other => panic!("empty filter: {other:?}"),
    }
}

/// Strict: V31-A-6 names no server reaction; closing relies on the server detecting it.
pub fn empty_topic_name_closes() {
    let broker = broker_or_skip!();
    let mut w = Writer::new();
    w.str("").raw(b"x");
    // req: 3.1/V31-A-6
    assert_violation_closes(&broker, &w.frame(0x30));
}

/// Strict: V31-A-8 names no server reaction; 3.1 has no rule that a receiver must close.
pub fn null_character_in_topic_name_closes() {
    let broker = broker_or_skip!();
    let mut w = Writer::new();
    w.str("conformance/a\0b").raw(b"x");
    // req: 3.1/V31-A-8
    assert_violation_closes(&broker, &w.frame(0x30));
}

/// Strict: V31-A-8 names no server reaction; 3.1 has no rule that a receiver must close.
pub fn null_character_in_topic_filter_closes() {
    let broker = broker_or_skip!();
    let mut w = Writer::new();
    w.u16(1).str("conformance/a\0b").u8(0);
    // req: 3.1/V31-A-8
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
    // req: 3.1/V31-3.3-2
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().topic,
        format!("{t}/y/z")
    );
}

pub fn multi_byte_utf8_strings_round_trip() {
    let broker = broker_or_skip!();
    let t = topic();
    // 2-, 3- and 4-byte sequences: the byte length (the 2-byte prefix)
    // differs from the character count.
    let name = format!("{t}/\u{e9}t\u{e9}/\u{20ac}/\u{1D11E}");
    assert_ne!(name.len(), name.chars().count());
    let mut exact = session(&broker);
    exact.subscribe(&name, 0).unwrap();
    let mut wild = session(&broker);
    wild.subscribe(&format!("{t}/+/\u{20ac}/#"), 0).unwrap();
    let mut publisher = session(&broker);
    publisher
        .publish(&name, "\u{fc}".as_bytes(), 0, false)
        .unwrap();
    for c in [&mut exact, &mut wild] {
        let p = c.expect_publish(default_timeout()).unwrap();
        // req: 3.1/V31-2.5-1
        // req: 3.1/V31-2.5-2
        assert_eq!(p.topic, name);
        assert_eq!(p.payload, "\u{fc}".as_bytes());
    }
}

pub fn topic_of_32767_characters_is_accepted() {
    let broker = broker_or_skip!();
    let mut name = format!("{}/", topic());
    while name.len() < 32_767 {
        name.push('a');
    }
    let mut sub = session(&broker);
    sub.subscribe(&name, 1).unwrap();
    let mut publisher = session(&broker);
    // req: 3.1/V31-2.2-18
    publisher.publish(&name, b"long", 1, false).unwrap();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 3.1/V31-2.2-18
    assert_eq!(
        (p.topic.len(), p.payload.as_slice()),
        (32_767, &b"long"[..])
    );
    assert_eq!(p.topic, name);
}

pub fn valid_wildcard_placements_match() {
    let broker = broker_or_skip!();
    let t = topic();
    let id = t.replace("conformance/", "");
    let single = crate::unique_id("lvl");
    // (filter, a topic it must match); with a shared broker only the
    // presence of that topic is checked.
    let cases = [
        ("#".to_owned(), t.clone()),
        (format!("{t}/#"), format!("{t}/a/b")),
        ("+".to_owned(), single),
        (format!("+/{id}"), t.clone()),
        (format!("{t}/+/b"), format!("{t}/a/b")),
        (format!("{t}/+"), format!("{t}/a")),
    ];
    let mut subs: Vec<_> = cases
        .iter()
        .map(|(filter, _)| {
            let mut c = session(&broker);
            let ack = c.subscribe(filter, 0).unwrap();
            assert!(
                ack.codes.len() == 1 && ack.codes[0] <= 2,
                "{filter:?}: {ack:?}"
            );
            c
        })
        .collect();
    let mut publisher = session(&broker);
    for (_, name) in &cases {
        publisher.publish(name, b"m", 0, false).unwrap();
    }
    for ((filter, name), c) in cases.iter().zip(&mut subs) {
        let found = loop {
            match c.expect_publish(default_timeout()) {
                Ok(p) if p.topic == *name => break true,
                Ok(_) => {}
                Err(_) => break false,
            }
        };
        // req: 3.1/V31-A-3
        // req: 3.1/V31-A-5
        assert!(found, "{filter:?} did not match {name:?}");
    }
}

pub fn misplaced_wildcards_never_match() {
    let broker = broker_or_skip!();
    let t = topic();
    // (filter uses '#', invalid filter, topics a lenient parser might match)
    let cases = [
        (
            true,
            format!("{t}/finance#"),
            vec![
                format!("{t}/finance"),
                format!("{t}/finance/x"),
                format!("{t}/financex"),
            ],
        ),
        (
            true,
            format!("{t}/#/b"),
            vec![format!("{t}/a/b"), format!("{t}/b")],
        ),
        (
            false,
            format!("{t}/finance+"),
            vec![format!("{t}/finance"), format!("{t}/financex")],
        ),
        (
            false,
            format!("{t}/+a/x"),
            vec![format!("{t}/a/x"), format!("{t}/ba/x")],
        ),
    ];
    // 3.1 allows a close or a normal-looking SUBACK for an invalid filter
    // (see `invalid_topic_filters_are_rejected`); either way it must not act
    // as a wildcard.
    let mut subs = Vec::new();
    for (is_hash, filter, _) in &cases {
        let mut c = session(&broker);
        let id = c.next_packet_id();
        c.send(&Packet::Subscribe(Subscribe {
            packet_id: id,
            properties: Vec::new(),
            filters: vec![(filter.clone(), 0)],
        }))
        .unwrap();
        match c.next_event(default_timeout()).unwrap() {
            crate::Event::Packet(Packet::SubAck(_)) => subs.push((*is_hash, filter, c)),
            crate::Event::Closed => {}
            other => panic!("filter {filter:?}: {other:?}"),
        }
    }
    let mut publisher = session(&broker);
    for name in cases.iter().flat_map(|(_, _, topics)| topics) {
        publisher.publish(name, b"m", 0, false).unwrap();
    }
    std::thread::sleep(quiet_period());
    for (is_hash, filter, mut c) in subs {
        let got = c
            .collect_publishes(std::time::Duration::from_millis(100))
            .unwrap();
        if is_hash {
            // req: 3.1/V31-A-3
            assert!(got.is_empty(), "invalid {filter:?} matched {got:?}");
        } else {
            // req: 3.1/V31-A-5
            assert!(got.is_empty(), "invalid {filter:?} matched {got:?}");
        }
    }
}
