// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Topic names, topic filters, wildcards and `$` topics (MQTT 5.0).

use crate::{Broker, Writer, broker_or_skip, default_timeout, quiet_period};

use super::{assert_error_closes, session, subscribe_rejected, topic};

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
    // req: 5.0/MQTT-3.3.2-3
    // req: 5.0/MQTT-4.5.0-1
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
    // req: 5.0/MQTT-3.3.2-3
    // req: 5.0/MQTT-4.5.0-1
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
    // req: 5.0/MQTT-3.3.2-3
    // req: 5.0/MQTT-4.5.0-1
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
    // req: 5.0/MQTT-4.7.3-4
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
        // req: 5.0/MQTT-3.3.2-3
        // req: 5.0/MQTT-4.5.0-1
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
    // If the broker does not route the `$` PUBLISH at all, the wildcard
    // subscribers' silence proves nothing.
    if exact.collect_publishes(quiet_period()).unwrap().is_empty() {
        crate::outcome::skipped(format!(
            "broker did not route the client PUBLISH to {dollar}"
        ));
        return;
    }
    for (name, c) in [("#", &mut hash), ("+/id", &mut plus)] {
        let got = c.collect_publishes(quiet_period()).unwrap();
        // req: 5.0/MQTT-4.7.2-1
        assert!(
            got.iter().all(|p| !p.topic.starts_with('$')),
            "{name} matched {got:?}"
        );
    }
}

/// Strict: MQTT-4.7.1-1/2 are filter syntax rules for the sender; rejecting relies on detection.
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
        // req: 5.0/MQTT-4.7.1-1
        // req: 5.0/MQTT-4.7.1-2
        // Any SUBACK failure code from the table, or an error close.
        subscribe_rejected(&mut c, &bad, 0);
    }
}

/// Strict: MQTT-4.7.3-1 names no server reaction; rejecting relies on detection.
pub fn empty_topic_filter_is_rejected() {
    let broker = broker_or_skip!();
    let mut c = session(&broker);
    // req: 5.0/MQTT-4.7.3-1
    subscribe_rejected(&mut c, "", 0);
}

/// Strict: MQTT-4.7.3-1 names no server reaction; closing relies on detection (MQTT-4.13.1-1).
pub fn empty_topic_name_closes() {
    let broker = broker_or_skip!();
    let mut w = Writer::new();
    w.str("").properties(&[]).raw(b"x");
    // req: 5.0/MQTT-4.7.3-1
    assert_error_closes(&broker, &w.frame(0x30));
}

/// U+0000 in a received packet "is a Malformed Packet" (MQTT-1.5.4-2): the
/// server must close (MQTT-4.13.1-1).
pub fn null_character_in_topic_name_closes() {
    let broker = broker_or_skip!();
    let mut w = Writer::new();
    w.str("conformance/a\0b").properties(&[]).raw(b"x");
    // req: 5.0/MQTT-4.7.3-2
    // req: 5.0/MQTT-1.5.4-2
    // req: 5.0/MQTT-4.13.1-1
    assert_error_closes(&broker, &w.frame(0x30));
}

/// U+0000 in a received packet "is a Malformed Packet" (MQTT-1.5.4-2): the
/// server must close (MQTT-4.13.1-1).
pub fn null_character_in_topic_filter_closes() {
    let broker = broker_or_skip!();
    let mut w = Writer::new();
    w.u16(1).properties(&[]).str("conformance/a\0b").u8(0);
    // req: 5.0/MQTT-4.7.3-2
    // req: 5.0/MQTT-1.5.4-2
    // req: 5.0/MQTT-4.13.1-1
    assert_error_closes(&broker, &w.frame(0x82));
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
    // req: 5.0/MQTT-3.3.2-3
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().topic,
        format!("{t}/y/z")
    );
}

pub fn ill_formed_utf8_topic_name_closes() {
    let broker = broker_or_skip!();
    for name in [&[b'a', 0xC3, 0x28][..], &[0xED, 0xA0, 0x80][..]] {
        let mut w = Writer::new();
        w.bin(name).properties(&[]).raw(b"x");
        // req: 5.0/MQTT-3.3.2-1
        assert_error_closes(&broker, &w.frame(0x30));
    }
}

pub fn byte_order_mark_in_a_topic_is_preserved() {
    let broker = broker_or_skip!();
    let t = topic();
    let with_bom = format!("{t}/\u{FEFF}bom");
    let without_bom = format!("{t}/bom");
    let mut exact = session(&broker);
    exact.subscribe(&with_bom, 0).unwrap();
    let mut stripped = session(&broker);
    stripped.subscribe(&without_bom, 0).unwrap();
    let mut publisher = session(&broker);
    publisher.publish(&with_bom, b"bom", 0, false).unwrap();
    let got = exact.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-1.5.4-3
    assert_eq!(got.topic.as_bytes(), with_bom.as_bytes(), "{got:?}");
    // A server that skipped the BOM would match the filter without it.
    // req: 5.0/MQTT-1.5.4-3
    stripped.expect_no_packet(quiet_period()).unwrap();
}
