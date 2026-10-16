// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Will messages beyond the basics in `lifecycle.rs` (MQTT 5.0).

use std::time::Duration;

use crate::{
    Connect, PropValue, Property, Will, broker_or_skip, default_timeout, frame, prop, quiet_period,
    unique_id,
};

use super::{SESSION_EXPIRY, V, connack_u32, session, topic};

pub fn will_is_published_at_its_qos() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    let granted = sub.subscribe(&t, 2).unwrap().codes[0];
    let mut will = Will::new(&t, b"q1-will");
    will.qos = 1;
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wq")).will(will))
        .unwrap();
    c.abort();
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.1.2-9
    assert_eq!(p.qos, granted.min(1), "{p:?}");
    assert_eq!(p.payload, b"q1-will");
}

pub fn will_without_retain_is_not_retained() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wnr")).will(Will::new(&t, b"plain")))
        .unwrap();
    c.abort();
    let p = sub.expect_publish(default_timeout()).unwrap();
    assert_eq!(p.payload, b"plain");
    // req: 5.0/MQTT-3.1.2-14
    assert!(!p.retain);
    let mut late = session(&broker);
    late.subscribe(&t, 0).unwrap();
    // req: 5.0/MQTT-3.1.2-14
    late.expect_no_packet(quiet_period()).unwrap();
}

pub fn will_is_published_after_keep_alive_expiry() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (c, ack) = broker
        .connect_ok(
            &Connect::new(V, &unique_id("wka"))
                .keep_alive(1)
                .will(Will::new(&t, b"timed-out")),
        )
        .unwrap();
    let keep_alive = connack_u32(&ack, prop::SERVER_KEEP_ALIVE, 1);
    if keep_alive == 0 || keep_alive > 3 {
        crate::outcome::skipped(format!("Server Keep Alive {keep_alive}"));
        return;
    }
    // Stay silent: the broker must close after 1.5 x keep-alive and publish
    // the will.
    let p = sub
        .expect_publish(Duration::from_millis(1_500 * u64::from(keep_alive)) + default_timeout())
        .unwrap();
    // req: 5.0/MQTT-3.1.2-8
    assert_eq!(p.payload, b"timed-out");
    drop(c);
}

pub fn will_is_published_when_the_server_closes_for_a_protocol_violation() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let (mut c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wpv")).will(Will::new(&t, b"violated")))
        .unwrap();
    // PUBLISH with QoS 3: the server must close the connection.
    c.send_raw(&frame(0x36, &[0, 1, b'a', 0, 1])).unwrap();
    let _ = c.expect_closed(default_timeout());
    let p = sub.expect_publish(default_timeout()).unwrap();
    // req: 5.0/MQTT-3.1.2-8
    assert_eq!(p.payload, b"violated");
}

pub fn will_is_published_only_once() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 1).unwrap();
    let id = unique_id("w1");
    let expiry = PropValue::FourByte(SESSION_EXPIRY);
    let (c, _) = broker
        .connect_ok(
            &Connect::new(V, &id)
                .clean(false)
                .property(prop::SESSION_EXPIRY_INTERVAL, expiry.clone())
                .will(Will::new(&t, b"once")),
        )
        .unwrap();
    c.abort();
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().payload,
        b"once"
    );
    // Reconnect without a will and drop again: the published will was
    // removed from the session, so nothing more is sent.
    let (c, _) = broker
        .connect_ok(
            &Connect::new(V, &id)
                .clean(false)
                .property(prop::SESSION_EXPIRY_INTERVAL, expiry),
        )
        .unwrap();
    c.abort();
    // req: 5.0/MQTT-3.1.2-10
    sub.expect_no_packet(quiet_period()).unwrap();
    let (c, _) = broker.connect_ok(&Connect::new(V, &id)).unwrap();
    c.disconnect().unwrap();
}

/// A will on `topic` with the given Will Delay Interval.
fn delayed_will(topic: &str, payload: &[u8], delay: u32) -> Will {
    let mut w = Will::new(topic, payload);
    w.properties.push(Property::new(
        prop::WILL_DELAY_INTERVAL,
        PropValue::FourByte(delay),
    ));
    w
}

/// CONNECT with Clean Start=0 and the given Session Expiry Interval.
fn with_expiry(client_id: &str, expiry: u32) -> Connect {
    Connect::new(V, client_id)
        .clean(false)
        .property(prop::SESSION_EXPIRY_INTERVAL, PropValue::FourByte(expiry))
}

pub fn will_delay_interval_postpones_the_will() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let id = unique_id("wdl");
    let (c, _) = broker
        .connect_ok(&with_expiry(&id, SESSION_EXPIRY).will(delayed_will(&t, b"late", 2)))
        .unwrap();
    c.abort();
    // req: 5.0/MQTT-3.1.2-8
    sub.expect_no_packet(Duration::from_millis(1_200)).unwrap();
    // req: 5.0/MQTT-3.1.2-8
    let p = sub
        .expect_publish(Duration::from_secs(1) + default_timeout())
        .unwrap();
    assert_eq!(p.payload, b"late");
    super::forget(&broker, &id);
}

pub fn reconnecting_before_the_will_delay_suppresses_the_will() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let id = unique_id("wdr");
    let (c, _) = broker
        .connect_ok(&with_expiry(&id, SESSION_EXPIRY).will(delayed_will(&t, b"never", 2)))
        .unwrap();
    c.abort();
    std::thread::sleep(Duration::from_millis(300));
    let (c, ack) = broker
        .connect_ok(&with_expiry(&id, SESSION_EXPIRY))
        .unwrap();
    assert!(ack.session_present());
    // req: 5.0/MQTT-3.1.3-9
    sub.expect_no_packet(Duration::from_secs(2) + quiet_period())
        .unwrap();
    c.disconnect().unwrap();
    super::forget(&broker, &id);
}

pub fn session_end_before_the_will_delay_publishes_the_will() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let id = unique_id("wde");
    // Will delay 60 s, but the session expires after 1 s: the will is
    // published when the session ends.
    let (c, _) = broker
        .connect_ok(&with_expiry(&id, 1).will(delayed_will(&t, b"session-over", 60)))
        .unwrap();
    c.abort();
    // req: 5.0/MQTT-3.1.2-8
    let p = sub
        .expect_publish(Duration::from_secs(1) + default_timeout())
        .unwrap();
    assert_eq!(p.payload, b"session-over");
}

pub fn will_delay_is_cut_short_by_session_expiry_0() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    // No Session Expiry Interval: the session ends with the connection, so
    // the will goes out at once despite its 60 s delay.
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wd0")).will(delayed_will(&t, b"now", 60)))
        .unwrap();
    c.abort();
    // req: 5.0/MQTT-3.1.2-8
    assert_eq!(
        sub.expect_publish(default_timeout()).unwrap().payload,
        b"now"
    );
}

pub fn will_properties_are_forwarded_with_user_property_order() {
    let broker = broker_or_skip!();
    let t = topic();
    let mut sub = session(&broker);
    sub.subscribe(&t, 0).unwrap();
    let mut will = Will::new(&t, b"{}");
    let user = [("k", "1"), ("a", "2"), ("k", "3"), ("z", "")];
    will.properties = vec![
        Property::new(prop::PAYLOAD_FORMAT_INDICATOR, PropValue::Byte(1)),
        Property::new(
            prop::CONTENT_TYPE,
            PropValue::Str("application/json".into()),
        ),
        Property::new(prop::RESPONSE_TOPIC, PropValue::Str(format!("{t}/reply"))),
        Property::new(prop::CORRELATION_DATA, PropValue::Bin(vec![0, 1, 2])),
    ];
    for (k, v) in user {
        will.properties.push(Property::new(
            prop::USER_PROPERTY,
            PropValue::Pair(k.into(), v.into()),
        ));
    }
    let (c, _) = broker
        .connect_ok(&Connect::new(V, &unique_id("wup")).will(will))
        .unwrap();
    c.abort();
    let p = sub.expect_publish(default_timeout()).unwrap();
    let got: Vec<PropValue> = super::props_of(&p.properties, prop::USER_PROPERTY);
    let want: Vec<PropValue> = user
        .iter()
        .map(|(k, v)| PropValue::Pair((*k).into(), (*v).into()))
        .collect();
    // req: 5.0/MQTT-3.1.3-10
    assert_eq!(got, want);
    // Will Properties travel with the will (section 3.1.3.2).
    assert_eq!(
        crate::find_prop(&p.properties, prop::CONTENT_TYPE),
        Some(&PropValue::Str("application/json".into()))
    );
    assert_eq!(
        crate::find_prop(&p.properties, prop::CORRELATION_DATA),
        Some(&PropValue::Bin(vec![0, 1, 2]))
    );
}
