<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright (c) 2026 SOFTT -->

# mqtt-testkit

`mqtt-testkit` is a conformance and interoperability test kit for MQTT brokers, by the OrbitMQ project. It
checks whether a broker follows the MQTT 3.1, 3.1.1 and 5.0 specifications and works the way real client
applications expect. It works with **any broker**: it talks to the broker over the network with its own MQTT encoder and decoder,
which share no code with any broker. The OrbitMQ project maintains it, but it doesn't depend on OrbitMQ.

- **Requirement-mapped.** Every normative statement in the specifications has an id (`5.0/MQTT-3.3.1-4`,
  `3.1.1/MQTT-3.1.2-2`, and derived `3.1/V31-…` ids for 3.1, which has none). Each case names the statements
  it checks, and a run reports a verdict for each one: met, violated, or inconclusive.
- **Honest coverage.** Every server-binding requirement is either checked by a case or listed in
  [`spec/applicability.tsv`](spec/applicability.tsv) with the reason it can't be checked from outside the
  broker. `mqtt-testkit coverage` shows both.
- **Interoperability scenarios.** 37 end-to-end client workflows for 3.1.1 and 5.0 (retained messages,
  wills, persistent sessions, keep-alive, topic aliases, flow control, shared subscriptions, …), written in
  Rust and run by default; no other tooling is needed.
- **External clients.** Also checks the broker with the Eclipse Mosquitto command-line clients
  (`mosquitto_pub` / `mosquitto_sub`), an independent client implementation.
- **No dependencies** beyond the Rust standard library.

## Broker results

Every week a [workflow](.github/workflows/brokers.yml) runs the full kit (strict tier included) against the
latest image of popular brokers and opens a pull request when the table below changes. A case counts as
failed only if it fails twice. Columns show conformance cases passed per version; "Strict tier" counts how
many client violations the broker detected and rejected (rejecting them is allowed but not required).

<!-- BEGIN broker-results -->

| Broker | Version | MQTT 3.1 | MQTT 3.1.1 | MQTT 5.0 | Requirements violated | Strict tier |
|---|---|---|---|---|---|---|
| [Eclipse Mosquitto](https://mosquitto.org) | `2.1.2` | ❌ 79/83 · 3 failed · 1 skipped | ❌ 136/140 · 2 failed · 2 skipped | ❌ 213/222 · 4 failed · 5 skipped | 14 | 34/38 rejected |
| [EMQX](https://www.emqx.com) | `6.3.2` | ❌ 75/83 · 7 failed · 1 skipped | ❌ 137/140 · 1 failed · 2 skipped | ❌ 208/222 · 3 failed · 11 skipped | 16 | 33/37 rejected |
| [HiveMQ CE](https://github.com/hivemq/hivemq-community-edition) | `24.04` | ❌ 76/83 · 4 failed · 3 skipped | ❌ 125/140 · 8 failed · 7 skipped | ❌ 202/222 · 5 failed · 15 skipped | 27 | 34/37 rejected |
| [NanoMQ](https://nanomq.io) | `0.25.7` | ❌ 61/83 · 20 failed · 2 skipped | ❌ 101/140 · 36 failed · 3 skipped | ❌ 157/222 · 55 failed · 10 skipped | 128 | 29/35 rejected |

<details><summary>Failing cases</summary>

**Eclipse Mosquitto** (9)

- `3.1/connect/client_id_longer_than_23_characters_gets_code_2`
- `3.1/connect/will_qos_and_retain_are_disregarded_without_will_flag`
- `3.1/malformed/subscribe_and_unsubscribe_with_dup_set_are_accepted`
- `3.1.1/connect/unsupported_protocol_level_gets_connack_0x01_then_close`
- `3.1.1/websocket/server_selects_mqtt_subprotocol`
- `5.0/flow/oversized_message_does_not_block_the_receive_window`
- `5.0/interop/will_delay_versus_session_expiry`
- `5.0/persistence/message_expiry_does_not_apply_once_a_qos2_publish_is_sent`
- `5.0/websocket/server_selects_mqtt_subprotocol`

**EMQX** (11)

- `3.1/connect/client_id_longer_than_23_characters_gets_code_2`
- `3.1/connect/password_flag_without_password_string_is_accepted`
- `3.1/connect/username_flag_without_username_string_is_accepted`
- `3.1/connect/will_qos_and_retain_are_disregarded_without_will_flag`
- `3.1/malformed/pubrel_with_dup_set_is_accepted`
- `3.1/malformed/subscribe_and_unsubscribe_with_dup_set_are_accepted`
- `3.1/pubsub/unauthorized_publish_and_subscribe_are_acknowledged_normally`
- `3.1.1/malformed/subscribe_with_invalid_requested_qos_closes`
- `5.0/connect/unsupported_authentication_method_is_refused`
- `5.0/flow/oversized_message_does_not_block_the_receive_window`
- `5.0/malformed/subscription_options_with_reserved_bits_close`

**HiveMQ CE** (17)

- `3.1/connect/client_id_longer_than_23_characters_gets_code_2`
- `3.1/connect/username_flag_without_username_string_is_accepted`
- `3.1/connect/will_qos_and_retain_are_disregarded_without_will_flag`
- `3.1/malformed/subscribe_and_unsubscribe_with_dup_set_are_accepted`
- `3.1.1/connect/ill_formed_utf8_user_name_closes`
- `3.1.1/connect/password_present_without_flag_closes`
- `3.1.1/connect/unsupported_protocol_level_gets_connack_0x01_then_close`
- `3.1.1/connect/username_present_without_flag_closes`
- `3.1.1/connect/will_fields_without_will_flag_close`
- `3.1.1/malformed/pubrel_with_invalid_fixed_header_flags_closes`
- `3.1.1/malformed/subscribe_with_ill_formed_utf8_filter_closes`
- `3.1.1/malformed/unsubscribe_with_ill_formed_utf8_filter_closes`
- `5.0/connect/password_present_without_flag_is_refused`
- `5.0/connect/unsupported_authentication_method_is_refused`
- `5.0/connect/username_present_without_flag_is_refused`
- `5.0/malformed/pubrel_with_invalid_fixed_header_flags_closes`
- `5.0/properties/retained_message_expiry_is_reduced_by_the_waiting_time`

**NanoMQ** (111)

- `3.1/connect/client_id_longer_than_23_characters_gets_code_2`
- `3.1/connect/empty_client_id_gets_code_2`
- `3.1/connect/will_qos_and_retain_are_disregarded_without_will_flag`
- `3.1/lifecycle/keep_alive_expiry_closes_and_publishes_the_will`
- `3.1/malformed/pubrel_with_dup_set_is_accepted`
- `3.1/persistence/inbound_qos2_state_survives_a_reconnect`
- `3.1/persistence/qos0_messages_queued_while_offline_are_noted`
- `3.1/persistence/qos1_and_qos2_messages_are_queued_while_offline`
- `3.1/persistence/redelivery_keeps_the_original_order`
- `3.1/persistence/unacknowledged_pubrel_is_resent_and_publish_is_not`
- `3.1/persistence/unacknowledged_qos1_is_resent_with_dup_and_the_same_identifier`
- `3.1/persistence/unacknowledged_qos2_publish_is_resent_with_dup`
- `3.1/pubsub/qos1_messages_from_one_publisher_keep_their_order`
- `3.1/pubsub/qos2_duplicate_before_pubrel_is_not_delivered_twice`
- `3.1/pubsub/qos2_messages_from_one_publisher_keep_their_order`
- `3.1/pubsub/qos2_onward_delivery_is_exactly_once_with_either_method`
- `3.1/pubsub/qos2_repeat_without_dup_is_recognised_by_its_message_id`
- `3.1/retain/empty_retained_message_is_delivered_to_current_subscribers`
- `3.1/will/will_is_published_after_keep_alive_expiry`
- `3.1/will/zero_length_will_message_is_published_empty`
- `3.1.1/connect/connect_without_client_id_field_closes`
- `3.1.1/connect/empty_client_id_without_clean_session_is_rejected_with_0x02`
- `3.1.1/connect/foreign_protocol_name_is_not_processed_as_mqtt`
- `3.1.1/connect/password_flag_without_password_closes`
- `3.1.1/connect/unsupported_protocol_level_gets_connack_0x01_then_close`
- `3.1.1/connect/username_flag_without_username_closes`
- `3.1.1/interop/empty_client_id_requires_clean_session`
- `3.1.1/interop/keep_alive_expiry_publishes_will`
- `3.1.1/interop/offline_queue_for_persistent_session`
- `3.1.1/interop/resend_unacknowledged_on_session_resume`
- `3.1.1/lifecycle/keep_alive_expiry_closes_the_connection`
- `3.1.1/malformed/disconnect_with_invalid_fixed_header_flags_closes`
- `3.1.1/malformed/pingreq_with_invalid_fixed_header_flags_closes`
- `3.1.1/malformed/reserved_packet_types_close`
- `3.1.1/malformed/server_to_client_packet_from_client_closes`
- `3.1.1/malformed/subscribe_with_invalid_fixed_header_flags_closes`
- `3.1.1/malformed/subscribe_without_topic_filters_closes`
- `3.1.1/malformed/unsubscribe_with_ill_formed_utf8_filter_closes`
- `3.1.1/malformed/unsubscribe_with_invalid_fixed_header_flags_closes`
- `3.1.1/persistence/inbound_qos2_state_survives_a_reconnect`
- `3.1.1/persistence/qos1_and_qos2_messages_are_queued_while_offline`
- `3.1.1/persistence/redelivery_keeps_the_original_order`
- `3.1.1/persistence/session_state_is_keyed_by_client_id`
- `3.1.1/persistence/unacknowledged_pubrel_is_resent_and_publish_is_not`
- `3.1.1/persistence/unacknowledged_qos1_is_resent_with_dup_and_the_same_identifier`
- `3.1.1/persistence/unacknowledged_qos2_publish_is_resent_with_dup`
- `3.1.1/persistence/unsubscribe_does_not_cancel_an_in_flight_qos1_delivery`
- `3.1.1/pubsub/inbound_dup_flag_is_not_propagated`
- `3.1.1/pubsub/qos0_messages_from_one_publisher_keep_their_order`
- `3.1.1/pubsub/qos2_duplicate_before_pubrel_is_not_delivered_twice`
- `3.1.1/pubsub/qos2_messages_from_one_publisher_keep_their_order`
- `3.1.1/retain/empty_retained_message_is_delivered_to_current_subscribers`
- `3.1.1/websocket/server_selects_mqtt_subprotocol`
- `3.1.1/websocket/several_packets_in_one_frame_are_processed`
- `3.1.1/will/will_is_published_after_keep_alive_expiry`
- `3.1.1/will/will_is_published_only_once`
- `5.0/connect/auth_without_authentication_method_is_a_protocol_error`
- `5.0/connect/foreign_protocol_name_is_refused`
- `5.0/connect/password_flag_without_password_is_refused`
- `5.0/connect/unsupported_authentication_method_is_refused`
- `5.0/connect/username_flag_without_username_is_refused`
- `5.0/flow/receive_maximum_counts_qos2_until_pubcomp`
- `5.0/flow/server_respects_the_clients_receive_maximum`
- `5.0/flow/topic_alias_mappings_do_not_survive_the_connection`
- `5.0/interop/inbound_topic_alias_rules`
- `5.0/interop/keep_alive_expiry_publishes_will`
- `5.0/interop/message_expiry_while_queued`
- `5.0/interop/offline_queue_for_expiring_session`
- `5.0/interop/resend_unacknowledged_on_session_resume`
- `5.0/interop/session_expiry_interval_lifecycle`
- `5.0/interop/will_delay_versus_session_expiry`
- `5.0/interop/will_on_keep_alive_timeout_with_properties`
- `5.0/lifecycle/keep_alive_expiry_closes_the_connection`
- `5.0/lifecycle/normal_disconnect_discards_the_will`
- `5.0/malformed/auth_with_invalid_fixed_header_flags_closes`
- `5.0/malformed/content_type_with_ill_formed_utf8_closes`
- `5.0/malformed/reserved_packet_type_closes`
- `5.0/malformed/response_topic_with_ill_formed_utf8_closes`
- `5.0/malformed/subscribe_with_invalid_fixed_header_flags_closes`
- `5.0/malformed/subscribe_without_topic_filters_closes`
- `5.0/malformed/topic_alias_zero_closes`
- `5.0/malformed/unsubscribe_filter_with_ill_formed_utf8_closes`
- `5.0/malformed/unsubscribe_with_invalid_fixed_header_flags_closes`
- `5.0/malformed/unsubscribe_without_topic_filters_closes`
- `5.0/malformed/user_property_with_ill_formed_utf8_closes`
- `5.0/persistence/client_identifier_selects_the_session`
- `5.0/persistence/inbound_qos2_state_survives_a_reconnect`
- `5.0/persistence/message_expiry_does_not_apply_once_a_qos2_publish_is_sent`
- `5.0/persistence/publish_rejected_with_an_error_ack_is_not_resent`
- `5.0/persistence/qos1_and_qos2_messages_are_queued_while_offline`
- `5.0/persistence/redelivery_keeps_the_original_order`
- `5.0/persistence/unacknowledged_pubrel_is_resent_and_publish_is_not`
- `5.0/persistence/unacknowledged_qos1_is_resent_with_dup_and_the_same_identifier`
- `5.0/persistence/unacknowledged_qos2_publish_is_resent_with_dup`
- `5.0/persistence/unsubscribe_completes_deliveries_already_started`
- `5.0/properties/assigned_client_identifiers_are_unique_and_resume_the_session`
- `5.0/properties/expired_queued_message_is_not_delivered`
- `5.0/properties/properties_survive_offline_queueing`
- `5.0/properties/queued_message_expiry_is_reduced_by_the_waiting_time`
- `5.0/properties/retained_message_expiry_is_reduced_by_the_waiting_time`
- `5.0/pubsub/inbound_dup_flag_is_not_propagated`
- `5.0/pubsub/qos0_messages_from_one_publisher_keep_their_order`
- `5.0/pubsub/qos2_duplicate_before_pubrel_is_not_delivered_twice`
- `5.0/pubsub/qos2_messages_from_one_publisher_keep_their_order`
- `5.0/shared/interrupted_qos2_shared_delivery_resumes_on_the_same_client`
- `5.0/websocket/server_selects_mqtt_subprotocol`
- `5.0/websocket/several_packets_in_one_frame_are_processed`
- `5.0/will/session_end_before_the_will_delay_publishes_the_will`
- `5.0/will/will_delay_interval_postpones_the_will`
- `5.0/will/will_is_published_after_keep_alive_expiry`
- `5.0/will/will_is_published_only_once`

</details>

<!-- END broker-results -->

## Install

Prebuilt binaries for Linux (x86_64, aarch64; static musl), macOS (Intel, Apple silicon) and Windows (x86_64,
aarch64) are attached to every [GitHub Release](https://github.com/OrbitMQ/mqtt-testkit/releases), with a
`SHA256SUMS` file. Or build from source:

```bash
cargo install --git https://github.com/OrbitMQ/mqtt-testkit mqtt-testkit
# or, from a checkout:
cargo build --release   # target/release/mqtt-testkit
```

## Quick start

```bash
# A running broker (must accept anonymous plain-TCP clients):
mqtt-testkit run --broker 127.0.0.1:1883

# Or let the suite start a fresh broker for every case on a free port ({port} is substituted):
mqtt-testkit run --spawn 'mosquitto -p {port}'
mqtt-testkit run --spawn '/path/to/orbitmq'          # ORBITMQ_LISTEN=127.0.0.1:<port> is also set

# What does the broker announce?
mqtt-testkit probe --broker 127.0.0.1:1883

# Explore interactively: versions → areas → cases → requirements, and run any of them
mqtt-testkit browse --broker 127.0.0.1:1883
```

A run prints failures as they happen, then a summary per version and the requirement verdicts. The exit
status is 0 when nothing failed, 1 on failures, and 2 on a usage or broker error.

```text
Results
  MQTT 3.1                 82 cases    82 passed    0 failed    0 skipped
  MQTT 3.1.1              105 cases   105 passed    0 failed    0 skipped
  MQTT 5.0                165 cases   163 passed    0 failed    2 skipped

Requirements 328 checked: 326 met, 0 violated, 2 inconclusive
✓ 350 passed, 2 skipped, no failures in 22.4 s
```

## Commands

| Command | What it does |
|---|---|
| `run [PATTERN…]` | Run cases (default: every conformance case) and report per-requirement verdicts |
| `list [PATTERN…] [-l]` | List cases by version and area; `-l` adds each case's requirements |
| `show <CASE \| REQ \| TEXT>` | Explain a case (description, source, requirements with spec links), a requirement (text and the cases that check it), or search both |
| `reqs [PATTERN…]` | Requirement catalogue with case counts; `--version`, `--area`, `--uncovered`, `--all` |
| `coverage [-v]` | Per-version coverage: checked, exempt (with reasons), open |
| `probe` | Connect with 3.1, 3.1.1 and 5.0 and show each CONNACK and the 5.0 capabilities |
| `interop [scenarios\|mosquitto]` | Run the interoperability checks |
| `browse` | Interactive navigator |
| `help [COMMAND]` | Options of a command |

**Patterns** select cases by id glob (`'5.0/connect/*'`, `'*will*'`), id prefix (`3.1.1`, `5.0/shared`) or
requirement id (`MQTT-3.3.1-4` in every version that has it, `5.0/MQTT-4.7.*`, `V31-3.1-4`). Exclude with
`-x`. Case ids are `<version>/<area>/<name>`.

```bash
mqtt-testkit run 5.0 -x 5.0/shared --broker localhost:1883     # 5.0 without shared subscriptions
mqtt-testkit run MQTT-3.1.2-4 --broker localhost:1883          # every case checking that statement
mqtt-testkit show 5.0/MQTT-3.3.2-9
mqtt-testkit list 3.1.1/pubsub -l
```

### Run options

| Option | Default | Meaning |
|---|---|---|
| `-b, --broker HOST:PORT` | `$MQTT_TESTKIT_BROKER` | Test a running broker |
| `-s, --spawn 'CMD ARGS'` | `$MQTT_TESTKIT_BIN` + `$MQTT_TESTKIT_BIN_ARGS` | Start a fresh broker per case; `{port}` and `{addr}` are substituted |
| `-x, --exclude PATTERN` | | Leave out cases (repeatable, comma-separated) |
| `-j, --jobs N` | CPUs, at most 8 | Cases run in parallel. Cases use unique client ids and topics, so a shared broker works |
| `--timeout-ms N` | 2000 | How long to wait for an expected broker reaction |
| `--quiet-ms N` | 1000 | How long to wait before concluding that something did not happen |
| `--case-timeout S` | 120 | Abandon a hung case after S seconds (reported as a failure) |
| `--fail-fast` | | Stop after the first failure |
| `--format text\|json\|junit` | text | Report format on stdout |
| `--json FILE`, `--junit FILE` | | Also write a JSON / JUnit XML report (for CI) |
| `-v` / `-q` | | One line per case with skip reasons and notes / only the summary |

Raise `--timeout-ms` and `--quiet-ms` for a remote or heavily loaded broker.

## Reading the results

- **passed**: every check in the case held.
- **failed**: a check failed. The message gives the observation and the source line, and the summary lists
  the requirements the case checks. `show <case>` explains it.
- **skipped**: the case does not apply to this broker. Either the broker legitimately doesn't offer the
  feature (for example a 5.0 broker announcing `Shared Subscription Available = 0`), or the check needs a
  limit the broker doesn't announce (for example Maximum Packet Size). Skips never count as passes.
- Requirement verdicts: **violated** if any case checking it failed; **met** if at least one passed and none
  failed; **inconclusive** if every case checking it was skipped.

A broker is conformant when a full run (no patterns) has no failures and every server-binding requirement is
met or inconclusive only because the broker doesn't offer an optional feature. Requirements in
`spec/applicability.tsv` can't be judged from outside the broker; review them against its documentation.

Where the specification allows several behaviours (for example a CONNACK with an error code, or just a
close), the suite accepts all of them. These judgment calls are listed in
[tests/README.md](tests/README.md#judgment-calls).

## Interoperability

The built-in scenarios are the `interop` area of each version (`3.1.1/interop/*`, `5.0/interop/*`) and run
with every `run`. A few adapt to the broker's setup: `subscribe_refused_by_policy` needs a filter the broker
denies (`MQTT_TESTKIT_INTEROP_DENY_FILTER`, otherwise it skips), and `server_keep_alive_override` checks a
configured Server Keep Alive when `MQTT_TESTKIT_INTEROP_SERVER_KEEP_ALIVE` names it.

The `interop` command runs the scenarios plus checks with the Mosquitto command-line clients, an
independent client implementation:

```bash
mqtt-testkit interop --spawn '/path/to/broker {port}'      # scenarios and Mosquitto clients
mqtt-testkit interop mosquitto --broker 127.0.0.1:1883
```

- **Mosquitto clients** (`interop/mosquitto/…`): `mosquitto_pub` / `mosquitto_sub` with `-V mqttv31`,
  `mqttv311` and `mqttv5`. Skipped when the clients aren't installed.

## Environment variables

Every option has an environment variable: `MQTT_TESTKIT_BROKER`, `MQTT_TESTKIT_BIN` / `MQTT_TESTKIT_BIN_ARGS`
(spawn), `MQTT_TESTKIT_WS`, `MQTT_TESTKIT_TIMEOUT_MS`, `MQTT_TESTKIT_QUIET_MS`,
`MQTT_TESTKIT_STARTUP_TIMEOUT_MS`, `MQTT_TESTKIT_VERIFY_OWNER` and `MQTT_TESTKIT_INTEROP_*`. The names used before the rename (`ORBITMQ_TEST_BROKER`, `ORBITMQ_BIN`, …) are
still read when the new one is unset.

## Using it from `cargo test`

Every case is also a `cargo test` test (generated by `build.rs`), for CI that prefers it:

```bash
MQTT_TESTKIT_BIN=/path/to/broker MQTT_TESTKIT_BIN_ARGS='-p {port}' cargo test --test v5
MQTT_TESTKIT_BROKER=127.0.0.1:1883 cargo test --test v3_1_1
cargo test        # no broker configured: every case skips and passes
```

## Layout

| Path | Contents |
|---|---|
| `src/suite/<v3_1\|v3_1_1\|v5>/<area>.rs` | The cases. Every top-level `pub fn` is a case (`build.rs` registers it) |
| `src/interop/` | Mosquitto command-line client checks |
| `src/codec.rs`, `src/client.rs`, `src/broker.rs` | Independent MQTT codec, raw-socket client, broker discovery and spawning |
| `src/registry.rs`, `src/runner.rs`, `src/report.rs`, `src/spec.rs` | Case registry and selection, parallel runner, reports, requirement catalogue |
| `src/bin/mqtt-testkit/` | The command-line interface |
| `spec/requirements.tsv` | Requirement catalogue: every normative statement, with level, binding and a link into the spec |
| `spec/applicability.tsv` | Server-binding requirements no black-box case can check, with the reason |
| `tests/` | `cargo test` wrappers and [the harness and contribution guide](tests/README.md) |

## Releasing

1. Set `version` in `Cargo.toml` (and refresh `Cargo.lock` with `cargo build`) and merge it.
2. Publish a GitHub Release whose tag is `v<version>` (e.g. `v0.1.0`).
3. The `release` workflow checks that the tag matches the crate version, runs the unit tests, builds the
   six targets, and uploads `mqtt-testkit-<tag>-<target>.tar.gz` / `.zip` (binary, LICENSE, NOTICE,
   README) plus `SHA256SUMS` to the release. Re-run it for an existing tag from the Actions tab
   (`workflow_dispatch`, input `tag`).

## Licence

Apache License 2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE). Requirement summaries are paraphrases; the OASIS and
IBM/Eurotech specification texts are authoritative.
