// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Mosquitto command-line client smoke tests (see `src/interop/mosquitto.rs`).
//! Skip without a broker or without `mosquitto_pub` / `mosquitto_sub`.

use mqtt_testkit::registry::run_for_cargo_test;

#[test]
fn mosquitto_clients_publish_and_subscribe() {
    for v in ["v3_1", "v3_1_1", "v5"] {
        run_for_cargo_test(&format!("interop/mosquitto/{v}_publish_and_subscribe"));
    }
}

#[test]
fn mosquitto_clients_retained_message() {
    for v in ["v3_1", "v3_1_1", "v5"] {
        run_for_cargo_test(&format!("interop/mosquitto/{v}_retained_message"));
    }
}
