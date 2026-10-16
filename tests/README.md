<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright (c) 2026 SOFTT -->

# Conformance cases: harness and contribution guide

Black-box cases that drive a broker over raw TCP (or WebSocket) using the crate's own MQTT encoder/decoder
(`src/codec.rs`). The harness shares no code with any broker, so a broker's codec bug can't hide itself.
For the command-line tool see the [top-level README](../README.md).

| Directory | Protocol | Contents |
|---|---|---|
| `src/suite/v3_1/` | MQTT 3.1 (`MQIsdp`, level 3) | CONNECT/CONNACK, malformed packets, keep-alive, PINGREQ, will, publish/subscribe, retain, topics, sessions |
| `src/suite/v3_1_1/` | MQTT 3.1.1 (`MQTT`, level 4) | same areas, plus the 3.1.1 fixed-header and payload rules, and WebSocket |
| `src/suite/v5/` | MQTT 5.0 (`MQTT`, level 5) | same areas, plus reason codes, properties, assigned client id, AUTH, subscription options, flow control, topic aliases, shared subscriptions, WebSocket |
| `src/interop/` | 3.1, 3.1.1, 5.0 | Mosquitto command-line client smoke tests (see [Interoperability](#interoperability)) |
| `tests/v3_1.rs`, `v3_1_1.rs`, `v5.rs`, `interop_mosquitto.rs` | | `cargo test` wrappers: one generated `#[test]` per case |

**A case is a top-level `pub fn name()` in an area file** (any file but `mod.rs`); `build.rs` registers it
with id `<version>/<area>/<name>`. Helpers are private `fn`s or live in `mod.rs`. A doc comment on the
function is shown by `mqtt-testkit show`. Area files:

| File | Contents |
|---|---|
| `connect.rs` | CONNECT/CONNACK |
| `malformed.rs` | malformed packets and protocol errors |
| `lifecycle.rs` | keep-alive, PINGREQ, DISCONNECT, basic wills |
| `pubsub.rs` | PUBLISH/SUBSCRIBE/UNSUBSCRIBE flows, QoS 0/1/2, QoS downgrade, packet ids, ordering |
| `retain.rs` | retained messages: set, replace, delete, RETAIN flag on delivery |
| `topics.rs` | wildcards, `$` topics, invalid names and filters, UTF-8 |
| `persistence.rs` | persistent sessions, Session Present, offline queueing, redelivery with DUP |
| `will.rs` | will QoS/retain, keep-alive and protocol-error wills; 5.0 Will Delay Interval |
| `interop.rs` | interoperability scenarios: end-to-end client workflows (3.1.1, 5.0) |
| `websocket.rs` | MQTT over WebSocket (3.1.1, 5.0; needs `--ws` / `MQTT_TESTKIT_WS`) |
| `v5/options.rs` | No Local, Retain As Published, Retain Handling, Subscription Identifiers |
| `v5/properties.rs` | property pass-through, Message Expiry, request/problem information, reason codes, assigned ids |
| `v5/flow.rs` | Receive Maximum, Maximum Packet Size, Topic Aliases |
| `v5/shared.rs` | shared subscriptions |

### Outcomes and tiers

- A case **fails** by panicking (`assert!`, `unwrap`).
- A case that does not apply to the broker calls `crate::outcome::skipped("reason")` and returns. Skips
  never count as passes; the requirements of an all-skipped case are *inconclusive*. Use it when the
  broker legitimately lacks a feature (announced in CONNACK, or detected from behaviour) or a needed limit.
- `crate::outcome::note("…")` records an observation that is not a failure.
- **Strict tier.** A case whose only basis is "the client violated a client-side rule, so the server must
  reject it" puts a doc line `/// Strict: <reason>` on the function. The specifications require rejection
  only once the server detects the violation (5.0 MQTT-4.13.1-1, 3.1.1 MQTT-4.8.0-1), so a compliant broker
  may fail such a case. Strict cases run only with `run --strict`. Rules with an explicit server MUST (e.g.
  ill-formed UTF-8 is a Malformed Packet, 5.0 MQTT-1.5.4-1; reserved flags; QoS 3 in 3.1.1 and 5.0) are not
  strict. MQTT 3.1 names almost no server reactions, so its cases built on a client rule (an invalid CONNECT,
  reserved values, wildcards in a topic name) are strict.

### Requirements without a case

Every server-binding requirement in `spec/requirements.tsv` is cited by a case or listed in
`spec/applicability.tsv` with a category (`not-black-box`, `needs-capability`, `informative`) and a reason.
`cargo test --lib` fails otherwise, and also when an exempted requirement gains a case.

## Choosing the broker under test

The harness picks the first match:

1. **`MQTT_TESTKIT_BIN=/path/to/orbitmq`**: each test spawns its own broker with
   `ORBITMQ_LISTEN=127.0.0.1:<free port>` and waits up to `MQTT_TESTKIT_STARTUP_TIMEOUT_MS` (default 10000) for
   the port to accept. The process is killed when the test ends. Choosing the port (bind port 0, release
   it) races with other tests and other processes, so port choice, spawn and readiness run under a
   process-wide lock. After the port accepts, the harness checks that the child is still alive and, where
   `lsof` exists, that the child's pid owns the listening socket. Otherwise (e.g. the broker exited with
   "address already in use", or the probe reached someone else's listener) the child is killed and the
   spawn is retried on a fresh port, up to 8 times. `MQTT_TESTKIT_VERIFY_OWNER=0` turns off the ownership check
   for wrappers whose listening process is not the spawned pid. `MQTT_TESTKIT_BIN_ARGS` adds extra
   whitespace-separated arguments, with `{addr}` and `{port}` substituted (so a third-party broker can run
   too, e.g. `MQTT_TESTKIT_BIN=mosquitto MQTT_TESTKIT_BIN_ARGS="-p {port}"`).
2. **`MQTT_TESTKIT_BROKER=host:port`**: use a broker that is already running. Tests use unique client IDs
   and topics, so a shared broker is fine. It must allow anonymous clients.
3. **The CLI's `--broker` / `--spawn`** override both (`broker::set_target`).
4. **WebSocket**: `--ws ws://host:port/path` or `MQTT_TESTKIT_WS` (no TLS). WebSocket cases use only this
   URL, also when brokers are spawned per case, and skip without it.
5. **Nothing is set**: every test prints `skipped: no broker (set MQTT_TESTKIT_BIN or MQTT_TESTKIT_BROKER)` and
   passes. CI relies on this, so `cargo test` works without a broker.

```bash
MQTT_TESTKIT_BIN=../broker/target/release/orbitmq cargo test
MQTT_TESTKIT_BROKER=127.0.0.1:1883 cargo test --test v5
MQTT_TESTKIT_BROKER=127.0.0.1:1883 cargo test --test v3_1_1 -- keep_alive --nocapture
```

Timing knobs:

| Variable | Default | Meaning |
|---|---|---|
| `MQTT_TESTKIT_TIMEOUT_MS` | 2000 | How long to wait for an expected reaction (a packet or a close). |
| `MQTT_TESTKIT_QUIET_MS` | 1000 | How long to wait before concluding that something did *not* happen (e.g. no will was published). |

The keep-alive tests use a 1 s keep-alive. Each one takes about 2 s of PINGREQ traffic, then waits for the
close for at most 1.5 × keep-alive plus `MQTT_TESTKIT_TIMEOUT_MS`.

## Requirement markers

Every assertion that proves a normative statement has a version-qualified marker on the line(s) just before
it, one per line:

```rust
// req: 3.1/V31-3.1-4
// req: 3.1.1/MQTT-3.1.2-2
// req: 5.0/MQTT-3.2.2-7
```

The prefix is the `spec:` field of `specs/requirements/mqtt-<version>.yaml` without `mqtt-`. Tag numbers are
reused across versions with different meanings, so an unqualified ID is never valid. Cite only what the test
actually proves. A test that only exercises a requirement in one branch (e.g. "if the server refuses a
zero-length client id it must use 0x02") puts the marker in that branch.

`scripts/req-check.py` in the control repo scans `conformance/` (and `broker/`) for these markers. It fails if
a marker names an unknown requirement, and it builds the compliance matrix (`--matrix`). With
`--enforce M1`, it also fails when an in-scope server-binding requirement has no test.

```bash
cd .. && python3 scripts/req-check.py
```

## Harness API (`mqtt_testkit`)

- `broker_or_skip!()`: returns the `Broker`, or prints the skip message and returns from the test.
- `Broker::open(version)` opens a raw connection. `Broker::connect(&Connect)` returns `(Client, ConnAck)`
  whatever the code. `Broker::connect_ok` requires code 0.
- `Client::send(&Packet)` and `Client::send_raw(&[u8])` send packets; `send_raw` is for deliberately
  malformed bytes. `codec::Writer` and `frame()` build packet bodies by hand.
- `Client::expect_packet(timeout)` waits for the next packet.
- `Client::expect_closed(timeout)` waits for the close and returns any packets received before it.
- `Client::expect_disconnect(timeout)` allows a bare close, or (in 5.0) one DISCONNECT and returns its
  reason code.
- `Client::expect_error_close(timeout)` is like `expect_disconnect`, but any DISCONNECT must carry a reason
  code ≥ 0x80.
- `Client::expect_refused(timeout)` accepts either a non-zero CONNACK followed by a close, or a close with no
  CONNACK.
- `Client::expect_no_packet(duration)` requires the connection to stay silent and open.
- `Client::ping`, `subscribe`, `unsubscribe`, `publish` (QoS 0–2 flows), `expect_publish`,
  `check_keep_alive`, `disconnect` and `abort` (drop without DISCONNECT).
- `Client::subscribe_many` / `unsubscribe_many` (several filters, 5.0 properties), `publish_packet` (any
  `Publish`; returns the PUBACK/PUBREC for reason codes), `expect_publish_unacked` (no acknowledgement),
  `collect_publishes(window)` and `expect_publishes(n, timeout)`. `expect_publish` acknowledges per QoS and
  answers a broker PUBREL with PUBCOMP.
- `Client::wait_for(timeout, what, pred)` waits for a specific packet. PUBLISH and PUBREL packets that
  arrive first are stashed and returned by later reads in arrival order, so a SUBACK or PUBACK wait is
  not upset by a delivery that overtakes it.
- `Broker::for_test()` is the `Option` form of `broker_or_skip!` for helpers that return values.
- WebSocket: `ws::endpoint_or_skip()` returns the configured URL or records a skip; `Client::open_ws(url,
  version)` gives a `Client` whose helpers all work over WebSocket; `Client::websocket()` exposes the
  `ws::WebSocket` for raw frames (`send_frame` with any opcode, `send_fragmented`, `send_binary_chunks`).
- v5 `mod.rs`: `Raw` returns every packet with its exact wire bytes (sizes, Property Length layout);
  `CONNACK_CODES` is the Connect Reason Code table.
- `default_timeout()`, `quiet_period()` and `unique_id(prefix)` (a valid ≤ 23-character client id or topic
  segment).

## Judgment calls

Where the specification allows several behaviours, the cases accept all of them.

- **Refusal shape.** When the spec allows either "CONNACK with an error code, then close" or "close", the case
  accepts both through `expect_refused`. The exceptions are where a CONNACK code is mandatory:
  - 3.1.1 unsupported level (0x01, MQTT-3.1.2-2)
  - 3.1.1 empty client id with CleanSession=0 (0x02)
  - 3.1 client id too long or empty (code 2)

  In 3.1.1, an invalid CONNECT must close *without* CONNACK (MQTT-3.1.4-1), so those cases reject any CONNACK.
  A case that needs a refusal only as a precondition (data sent after a refused CONNECT, the CONNACK code
  table, a refusing CONNACK's size) skips that refusal when the broker accepts the CONNECT; the case that
  owns the refusal reports it.
- **Unknown protocol level or name (5.0 cases).** A multi-version server may answer a CONNECT that is not
  `MQTT` level 5 under another version's rules. Level ≠ 5 may get CONNACK 0x84 or a 3.x-style 0x01; a foreign
  protocol name may get any non-zero code. The close is always mandatory. Levels 0–2 with `MQTT` are tested
  under 3.1.1 rules (CONNACK 0x01): a 3.1.1 CONNACK 0x01 satisfies both specifications, a 5.0 CONNACK 0x84
  does not satisfy MQTT-3.1.2-2 of 3.1.1. A 3.1 `MQIsdp` CONNECT with level 5 (sent with a Property Length)
  may be answered as a 5.0 CONNECT with a foreign name: a 5.0 CONNACK with any error code, then the close.
- **5.0 DISCONNECT before close** is optional. If present, it must carry a reason code ≥ 0x80, from the
  DISCONNECT table. For a session takeover it must be 0x8E. After a DISCONNECT the server sends nothing more.
  An unmapped Topic Alias needs only the close; any error code may come with it.
- **DISCONNECT to a 3.x client.** 3.x has no server-to-client DISCONNECT. Where a case requires a close, a
  5.0-style DISCONNECT immediately before it is noted, not failed: it does not answer the client's packet.
- **First packet other than CONNECT.** MQTT-3.1.0-1 binds the client, and the server only SHOULD close a
  connection that sends no CONNECT. The normal-tier case requires that nothing but a refusing CONNACK is sent
  (MQTT-3.2.0-1, 5.0 MQTT-3.14.0-1); the strict-tier case requires the close, waiting up to 30 s because it
  may come at the server's own CONNECT timeout.
- **In-flight window.** A server may keep fewer QoS > 0 messages in flight than the client's Receive Maximum
  allows, down to one at a time. Cases read what is in flight and acknowledge to get the rest; a check that
  needs two messages in flight at once (distinct identifiers, PUBREL order, unsubscribing two started
  deliveries) skips when the server sends one at a time. A delivery released by a timer after the window
  frees is accepted, but it is a first transmission (DUP = 0).
- **PUBREL after PUBREC ≥ 0x80** is not required (MQTT-4.3.3-4 asks for one only below 0x80) and not
  forbidden; it is answered with PUBCOMP and noted.
- **Shared subscriptions at QoS 2.** A server may grant QoS 1 on a shared subscription; the interrupted
  QoS 2 delivery cases (MQTT-4.8.2-4/5) skip then.
- **Variable Byte Integers** (5.0 MQTT-1.5.5-1) bind the encoder: the server's own remaining lengths and
  property lengths must be minimal. Rejecting a client's non-minimal encoding is strict tier.
- **Ill-formed UTF-8** in any string field is a Malformed Packet (3.1.1 MQTT-1.5.3-1, 5.0 MQTT-1.5.4-1): the
  server must close or refuse; a SUBACK failure code is not enough. A U+FEFF byte order mark must be kept.
- **QoS 2 onward delivery** may happen on PUBLISH or on PUBREL (Method A or B; 3.1 §4.1). Cases complete every
  QoS 2 flow (PUBREL in PUBREC order) before asserting delivery, keep in-flight messages within the server's
  Receive Maximum, and require exactly one onward copy. PUBCOMP order is not asserted.
- **Delivered QoS** must equal the minimum of the published QoS and the granted QoS. A server may grant less
  than requested; cases compare with what was granted, and a case that needs the requested QoS (to check a
  QoS 1/2 flow or redelivery) skips when the grant is lower (`Client::subscribe_needing`).
- **Overlapping subscriptions** (3.1.1 MQTT-3.3.5-1, 5.0 MQTT-3.3.4-2/4/5): one copy at the highest QoS, or
  one copy per subscription, are both accepted. The highest granted QoS and every Subscription
  Identifier must show up.
- **Invalid topic filters**: 3.1.1 accepts SUBACK 0x80 or a close. 5.0 accepts any SUBACK failure code from
  the table (0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2) or an error close. 3.1 has no failure
  code, so it accepts a close or a SUBACK granting QoS 0–2 (never 0x80, V31-3.9-3), and a granted filter
  with a misplaced wildcard must never match as a wildcard. These cases are strict tier.
- **Wildcard support (3.1.1)**: a server may reject wildcard filters (0x80 or close); a granted one must match.
- **Reason-code choices**: UNSUBACK for a filter that had no subscription may be 0x11 or 0x00. PUBACK and
  PUBREC may carry any code from their tables. PUBCOMP for an unknown PUBREL may be 0x92 or 0x00, but after
  a message expired mid-flow it must be 0x00. Topic Alias 0 may be 0x94 or 0x82. An alias above the maximum
  must be 0x94, Receive Maximum exceeded 0x93 (or a PUBREC ≥ 0x80 for the excess PUBLISH), Maximum Packet
  Size exceeded 0x95, QoS above Maximum QoS 0x9B and RETAIN without Retain Available 0x9A, each only if a
  DISCONNECT is sent. A Will QoS or Will Retain beyond the server's capability may be refused with any error
  code (0x9B / 0x9A are only recommended).
- **Maximum Packet Size (client's)**: every packet the server sends must fit; the server may drop Reason
  Strings and User Properties or never send them.
- **Announced capabilities (5.0)**: a QoS 1/2 or retained PUBLISH above what CONNACK announced (Maximum QoS,
  Retain Available), a wildcard SUBSCRIBE without Wildcard Subscription Available (or answered 0xA2), and a
  Will refused with 0x9B / 0x9A end the case as skipped: the broker would rightly treat them as errors. The
  client enforces this centrally; cases that send such a packet on purpose use `send_raw`.
- **Capability-dependent cases** skip when the broker offers the feature they need it to lack (Maximum QoS,
  Retain Available, Topic Alias Maximum, wildcard support) or lacks a limit they need (Receive Maximum 65535,
  no Maximum Packet Size), and when the broker doesn't reject what they need rejected (authentication,
  PUBREC ≥ 0x80).
- **Authorization probes**: `$SYS/conformance/<id>` is used to look for a topic the broker does not
  authorize; if the message gets through, the case skips. 3.1.1 accepts a PUBACK or a close; 3.1 requires a
  normal PUBACK and SUBACK. If a PUBLISH to any `$` topic is refused, not only to `$SYS`, the broker
  applies a reserved-topic rule (3.1.1 §4.7.2), not an authorization decision, and the 3.1 case skips.
  Cases that send credentials skip if the broker refuses them; a 3.1 refusal must be CONNACK 4 or 5.
- **Empty client id (5.0)**: a server may refuse it (0x85 then close, or a bare close); the assigned-id cases
  skip then.
- **Payload Format Indicator 0** may be forwarded as absent, since absent means 0.
- **Retained QoS 0**: a server MAY discard a retained QoS 0 message at any time (MQTT-3.3.1-7), so where a
  scenario publishes one, its absence later is noted, not failed; retained Wills that must be found are QoS 1.
- **Will timing (5.0)**: MQTT-3.1.2-8 requires the Will after the Will Delay or the session end, whichever is
  first; publishing promptly is a SHOULD. An early Will fails; a late one is noted.
- **DISCONNECT from the client** is followed by closing the client's side (the client MUST close; the server
  only SHOULD), so cases never wait on the server to close a connection the client should end.
- **Session takeover and wills (5.0)**: whether the old connection's will is published when a new
  connection takes over the client id is ambiguous (MQTT-3.1.2-8 "new Network Connection before the
  Will Delay Interval" vs §3.1.4), so it is not tested.
- **3.1 `$` topics** (V31-A-9), 3.1 retained-message deletion (V31-2.1-10) and 3.1.1 QoS 0 offline queueing
  are MAY behaviours and are not asserted. 3.1 QoS 0 offline queueing is asserted to be absent (V31-2.2-5).
- **Ordering** binds only per topic and QoS (MQTT-4.6.0-5/6): messages queued at different QoS levels are
  matched by payload, and their order is checked only when they arrive at the same QoS.
- **Strict-tier failures** are client violations the broker did not reject. They never make a requirement
  "violated", are counted separately (`strict_not_rejected` in the JSON summary) and do not fail the run.
- **WebSocket**: responses to several packets in one frame may come in any order the spec allows; the server
  must select subprotocol `mqtt` and close on a text frame.
- **No fixed sleeps for ordering across connections**: retained publishes that a later subscriber must find
  are QoS 1 (wait for PUBACK); topics are unique per case, so a shared broker works, and cases that
  subscribe to `#` or `+/+` only check their own topic.

## Interoperability

### Scenarios (`src/suite/v3_1_1/interop.rs`, `src/suite/v5/interop.rs`)

End-to-end client workflows (retained messages, wills, persistent sessions, keep-alive, topic aliases, flow
control, shared subscriptions, ...). They are ordinary cases of the `interop` area and run with every `run`.
`MQTT_TESTKIT_INTEROP_DENY_FILTER` names a filter the broker refuses (else the policy scenario skips);
`MQTT_TESTKIT_INTEROP_SERVER_KEEP_ALIVE` names a Server Keep Alive the broker is configured to announce.

### Mosquitto command-line clients (`src/interop/mosquitto.rs`)

Publishes and subscribes at QoS 0/1/2 and sets, reads and clears a retained message with `-V mqttv31`,
`mqttv311` and `mqttv5`. Skips when `mosquitto_pub`/`mosquitto_sub` are not on `PATH` (Debian/Ubuntu:
`apt-get install mosquitto-clients`; macOS: `brew install mosquitto`). The 3.1.1 publish/subscribe check
carries `// req: 3.1.1/MQTT-7.0.0-2` (interoperation with an independent client without extensions).

```bash
MQTT_TESTKIT_BIN=../broker/target/release/orbitmq cargo test --test interop_mosquitto -- --nocapture
mqtt-testkit interop mosquitto --spawn ../broker/target/release/orbitmq
```
