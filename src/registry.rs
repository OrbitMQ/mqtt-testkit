// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! The table of conformance cases, generated at build time from
//! `src/suite/<version>/<area>.rs` (see `build.rs`), and case selection.

use crate::codec::Version;

/// One conformance case.
#[derive(Debug, Clone, Copy)]
pub struct Case {
    /// `<spec version>/<area>/<name>`, e.g. `5.0/connect/client_id_of_23_alphanumerics_is_accepted`.
    pub id: &'static str,
    /// Protocol version the case speaks.
    pub version: Version,
    /// Area (the source file stem): `connect`, `pubsub`, ...
    pub area: &'static str,
    /// Function name.
    pub name: &'static str,
    /// Doc comment, if any.
    pub doc: &'static str,
    /// Version-qualified requirement IDs the case checks, e.g. `5.0/MQTT-3.1.2-1`.
    pub reqs: &'static [&'static str],
    /// Strict tier: the case checks that the server detects and rejects a
    /// client's violation of a client-side rule. The spec makes rejection
    /// mandatory only once the server detects the violation (5.0
    /// MQTT-4.13.1-1, 3.1.1 MQTT-4.8.0-1), so a compliant broker may fail it.
    /// Marked by a doc line starting with `Strict:`; run with `--strict`.
    pub strict: bool,
    /// Source file, relative to the crate root.
    pub file: &'static str,
    /// Line of the function in [`Case::file`].
    pub line: usize,
    /// The case. It panics on failure and calls
    /// [`outcome::skipped`](crate::outcome::skipped) when it cannot run.
    pub run: fn(),
}

impl Case {
    /// The spec version label: `3.1`, `3.1.1` or `5.0`.
    #[must_use]
    pub fn spec(&self) -> &'static str {
        spec_label(self.version)
    }

    /// The function name as words: `client_id_is_kept` -> `client id is kept`.
    #[must_use]
    pub fn title(&self) -> String {
        self.name.replace('_', " ")
    }
}

include!(concat!(env!("OUT_DIR"), "/registry.rs"));

/// Every conformance case (not the interop cases).
#[must_use]
pub fn cases() -> &'static [Case] {
    CASES
}

/// Every case: the conformance cases, then the interop cases.
pub fn all() -> impl Iterator<Item = &'static Case> {
    CASES.iter().chain(crate::interop::CASES.iter())
}

/// Whether `case` is an interoperability case (`interop/...`).
#[must_use]
pub fn is_interop(case: &Case) -> bool {
    case.id.starts_with("interop/")
}

/// The case with this exact id.
#[must_use]
pub fn find(id: &str) -> Option<&'static Case> {
    all().find(|c| c.id == id)
}

/// The spec label for `version`.
#[must_use]
pub const fn spec_label(version: Version) -> &'static str {
    match version {
        Version::V31 => "3.1",
        Version::V311 => "3.1.1",
        Version::V5 => "5.0",
    }
}

/// The version for a spec label (`3.1`, `3.1.1`, `5.0`; also `5`, `311`, `31`).
#[must_use]
pub fn parse_spec(label: &str) -> Option<Version> {
    match label.trim().trim_start_matches(['v', 'V']) {
        "3.1" | "31" => Some(Version::V31),
        "3.1.1" | "311" => Some(Version::V311),
        "5.0" | "5" | "50" => Some(Version::V5),
        _ => None,
    }
}

/// Every spec version, oldest first.
pub const VERSIONS: [Version; 3] = [Version::V31, Version::V311, Version::V5];

/// Human title of an area.
#[must_use]
pub fn area_title(area: &str) -> &str {
    match area {
        "connect" => "CONNECT / CONNACK",
        "malformed" => "Malformed packets and protocol errors",
        "lifecycle" => "Keep-alive, PINGREQ, DISCONNECT",
        "pubsub" => "PUBLISH / SUBSCRIBE and QoS flows",
        "retain" => "Retained messages",
        "topics" => "Topic names, filters and wildcards",
        "persistence" => "Sessions and offline delivery",
        "will" => "Will messages",
        "options" => "Subscription options",
        "properties" => "Properties and reason codes",
        "flow" => "Flow control, packet size, topic aliases",
        "shared" => "Shared subscriptions",
        "websocket" => "MQTT over WebSocket",
        "interop" => "Interoperability scenarios (end-to-end client workflows)",
        other => other,
    }
}

/// The areas of `version`, in registry order, with their case counts.
#[must_use]
pub fn areas(version: Version) -> Vec<(&'static str, usize)> {
    let mut out: Vec<(&'static str, usize)> = Vec::new();
    for c in CASES.iter().filter(|c| c.version == version) {
        match out.iter_mut().find(|(a, _)| *a == c.area) {
            Some((_, n)) => *n += 1,
            None => out.push((c.area, 1)),
        }
    }
    out
}

/// Selects cases by pattern. A pattern matches a case when it is:
///
/// - a glob over the case id (`*` matches any run of characters, `?` one):
///   `5.0/connect/*`, `*will*`;
/// - an id prefix at a `/` boundary: `5.0`, `3.1.1/pubsub`;
/// - an area name, in every version: `will`, `websocket`;
/// - a requirement id, qualified or not, possibly a glob: `5.0/MQTT-3.1.2-1`,
///   `MQTT-3.3.1-*`, `V31-3.1-4`. It matches the cases citing it.
///
/// No patterns select every conformance case (interop cases are selected
/// only by an `interop...` pattern or a requirement). `exclude` patterns
/// remove cases.
#[must_use]
pub fn select(include: &[String], exclude: &[String]) -> Vec<&'static Case> {
    all()
        .filter(|c| {
            if include.is_empty() {
                !is_interop(c)
            } else {
                include.iter().any(|p| matches(p, c))
            }
        })
        .filter(|c| !exclude.iter().any(|p| matches(p, c)))
        .collect()
}

/// Whether `pattern` selects `case` (see [`select`]).
#[must_use]
pub fn matches(pattern: &str, case: &Case) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return false;
    }
    if is_requirement_pattern(pattern) {
        return case.reqs.iter().any(|r| {
            glob(pattern, r) || r.split_once('/').is_some_and(|(_, id)| glob(pattern, id))
        });
    }
    if glob(pattern, case.id) {
        return true;
    }
    // A bare area name (`websocket`, `will`) selects that area in every version.
    if !pattern.contains(['/', '*', '?']) && case.area == pattern && !is_interop(case) {
        return true;
    }
    let prefix = pattern.trim_end_matches('/');
    case.id
        .strip_prefix(prefix)
        .is_some_and(|rest| rest.starts_with('/'))
}

/// Whether `pattern` names requirements rather than cases.
#[must_use]
pub fn is_requirement_pattern(pattern: &str) -> bool {
    let last = pattern.rsplit('/').next().unwrap_or(pattern);
    last.starts_with("MQTT-") || last.starts_with("V31-")
}

/// Glob match: `*` is any run of characters (including `/`), `?` is one.
#[must_use]
pub fn glob(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        match p.get(pi) {
            Some('*') => {
                star = Some((pi, ti));
                pi += 1;
            }
            Some(&c) if c == '?' || t.get(ti) == Some(&c) => {
                pi += 1;
                ti += 1;
            }
            _ => match star {
                Some((sp, st)) => {
                    pi = sp + 1;
                    ti = st + 1;
                    star = Some((sp, st + 1));
                }
                None => return false,
            },
        }
    }
    p.get(pi..)
        .is_some_and(|rest| rest.iter().all(|&c| c == '*'))
}

/// Runs the case `id` directly (no panic capture), for the generated
/// `cargo test` wrappers in `tests/`.
///
/// # Panics
///
/// If the case fails, or `id` is unknown.
#[allow(
    clippy::panic,
    reason = "cargo test wrapper: a failing case fails the test"
)]
pub fn run_for_cargo_test(id: &str) {
    let Some(case) = find(id) else {
        panic!("unknown conformance case {id}");
    };
    (case.run)();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_populated_and_ids_are_unique() {
        assert!(CASES.len() > 250, "{}", CASES.len());
        let mut ids: Vec<&str> = CASES.iter().map(|c| c.id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), CASES.len());
        for v in VERSIONS {
            assert!(CASES.iter().any(|c| c.version == v));
        }
    }

    #[test]
    fn every_requirement_marker_matches_the_case_version() {
        for c in CASES {
            for r in c.reqs {
                assert!(
                    r.starts_with(&format!("{}/", c.spec())),
                    "{} cites {r}",
                    c.id
                );
            }
        }
    }

    #[test]
    fn glob_semantics() {
        assert!(glob("5.0/*", "5.0/connect/x"));
        assert!(glob("*will*", "3.1/will/will_is_sent"));
        assert!(glob("a?c", "abc"));
        assert!(!glob("a?c", "ac"));
        assert!(glob("*", ""));
        assert!(!glob("3.1/*", "3.1.1/connect/x"));
        assert!(glob("MQTT-3.1.*", "MQTT-3.1.2-1"));
    }

    #[test]
    fn version_prefix_does_not_cross_versions() {
        let only_31 = select(&["3.1".into()], &[]);
        assert!(!only_31.is_empty());
        assert!(only_31.iter().all(|c| c.version == Version::V31));
        let area = select(&["5.0/connect".into()], &[]);
        assert!(
            area.iter()
                .all(|c| c.version == Version::V5 && c.area == "connect")
        );
    }

    #[test]
    fn requirement_patterns_select_citing_cases() {
        let qualified = select(&["5.0/MQTT-3.1.2-2".into()], &[]);
        assert!(!qualified.is_empty());
        assert!(
            qualified
                .iter()
                .all(|c| c.reqs.contains(&"5.0/MQTT-3.1.2-2"))
        );
        // Unqualified ids match in every version that has the tag.
        let unqualified = select(&["MQTT-3.1.2-2".into()], &[]);
        assert!(unqualified.len() >= qualified.len());
        assert!(unqualified.iter().any(|c| c.version == Version::V311));
    }

    #[test]
    fn interop_cases_need_an_explicit_pattern() {
        assert!(select(&[], &[]).iter().all(|c| !is_interop(c)));
        // `interop` selects the external-client runners and every version's interop area.
        let interop = select(&["interop".into()], &[]);
        let areas = CASES.iter().filter(|c| c.area == "interop").count();
        assert_eq!(interop.len(), crate::interop::CASES.len() + areas);
        assert_eq!(select(&["interop/mosquitto".into()], &[]).len(), 6);
        assert!(find("interop/mosquitto/v5_retained_message").is_some());
    }

    #[test]
    fn bare_area_names_select_the_area_in_every_version() {
        let will = select(&["will".into()], &[]);
        assert!(will.iter().all(|c| c.area == "will"));
        for v in VERSIONS {
            assert!(will.iter().any(|c| c.version == v));
        }
    }

    #[test]
    fn exclusion_removes_cases() {
        let all = select(&["5.0".into()], &[]);
        let fewer = select(&["5.0".into()], &["5.0/shared".into()]);
        assert!(fewer.len() < all.len());
        assert!(fewer.iter().all(|c| c.area != "shared"));
    }

    #[test]
    fn spec_labels_round_trip() {
        for v in VERSIONS {
            assert_eq!(parse_spec(spec_label(v)), Some(v));
        }
        assert_eq!(parse_spec("v5"), Some(Version::V5));
        assert_eq!(parse_spec("4"), None);
    }
}
