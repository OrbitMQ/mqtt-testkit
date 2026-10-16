// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Interoperability with independent client implementations: the Eclipse
//! Mosquitto command-line clients. Each check is a [`Case`] like the
//! conformance cases, with an `interop/...` id. (The built-in interoperability
//! scenarios are the `interop` area of each version under `src/suite/`.)
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    reason = "interop cases: a failed unwrap or assertion is a test failure"
)]

pub mod mosquitto;

use crate::codec::Version;
use crate::registry::Case;

macro_rules! interop_case {
    ($id:literal, $version:ident, $area:literal, $name:literal, $doc:literal, [$($req:literal),*], $run:path) => {
        Case {
            id: $id,
            version: Version::$version,
            area: $area,
            name: $name,
            doc: $doc,
            reqs: &[$($req),*],
            strict: false,
            file: file!(),
            line: line!() as usize,
            run: $run,
        }
    };
}

/// Every interoperability case.
pub static CASES: &[Case] = &[
    interop_case!(
        "interop/mosquitto/v3_1_publish_and_subscribe",
        V31,
        "mosquitto",
        "v3_1_publish_and_subscribe",
        "mosquitto_pub / mosquitto_sub -V mqttv31 at QoS 0, 1 and 2.",
        [],
        mosquitto::v3_1_publish_and_subscribe
    ),
    interop_case!(
        "interop/mosquitto/v3_1_1_publish_and_subscribe",
        V311,
        "mosquitto",
        "v3_1_1_publish_and_subscribe",
        "mosquitto_pub / mosquitto_sub -V mqttv311 at QoS 0, 1 and 2.",
        ["3.1.1/MQTT-7.0.0-2"],
        mosquitto::v3_1_1_publish_and_subscribe
    ),
    interop_case!(
        "interop/mosquitto/v5_publish_and_subscribe",
        V5,
        "mosquitto",
        "v5_publish_and_subscribe",
        "mosquitto_pub / mosquitto_sub -V mqttv5 at QoS 0, 1 and 2.",
        [],
        mosquitto::v5_publish_and_subscribe
    ),
    interop_case!(
        "interop/mosquitto/v3_1_retained_message",
        V31,
        "mosquitto",
        "v3_1_retained_message",
        "Set, read and clear a retained message with the mosquitto clients, -V mqttv31.",
        [],
        mosquitto::v3_1_retained_message
    ),
    interop_case!(
        "interop/mosquitto/v3_1_1_retained_message",
        V311,
        "mosquitto",
        "v3_1_1_retained_message",
        "Set, read and clear a retained message with the mosquitto clients, -V mqttv311.",
        [],
        mosquitto::v3_1_1_retained_message
    ),
    interop_case!(
        "interop/mosquitto/v5_retained_message",
        V5,
        "mosquitto",
        "v5_retained_message",
        "Set, read and clear a retained message with the mosquitto clients, -V mqttv5.",
        [],
        mosquitto::v5_retained_message
    ),
];
