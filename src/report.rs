// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Run summaries, per-requirement verdicts, and JSON / `JUnit` XML reports.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use crate::registry::spec_label;
use crate::runner::{CaseResult, Status};
use crate::spec;

/// Case counts.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    /// Cases that ran (any status).
    pub total: usize,
    /// Passed.
    pub passed: usize,
    /// Failed.
    pub failed: usize,
    /// Skipped.
    pub skipped: usize,
    /// Timed out.
    pub timed_out: usize,
}

impl Summary {
    /// Counts `results`.
    #[must_use]
    pub fn of(results: &[CaseResult]) -> Self {
        let mut s = Self {
            total: results.len(),
            ..Self::default()
        };
        for r in results {
            match r.status {
                Status::Passed => s.passed += 1,
                Status::Failed => s.failed += 1,
                Status::Skipped => s.skipped += 1,
                Status::TimedOut => s.timed_out += 1,
            }
        }
        s
    }

    /// Whether nothing failed or timed out.
    #[must_use]
    pub const fn ok(&self) -> bool {
        self.failed == 0 && self.timed_out == 0
    }
}

/// What a run says about one requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Verdict {
    /// A case citing it failed or timed out.
    Violated,
    /// Every case citing it that ran was skipped (the broker does not offer
    /// the feature, or the check needs a configuration the broker lacks).
    Inconclusive,
    /// At least one citing case passed and none failed.
    Met,
}

impl Verdict {
    /// Lower-case label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Violated => "violated",
            Self::Inconclusive => "inconclusive",
            Self::Met => "met",
        }
    }
}

/// The verdict for every requirement cited by a case in `results`.
#[must_use]
pub fn verdicts(results: &[CaseResult]) -> BTreeMap<&'static str, Verdict> {
    let mut out: BTreeMap<&'static str, Verdict> = BTreeMap::new();
    for r in results {
        let v = match r.status {
            Status::Failed | Status::TimedOut => Verdict::Violated,
            Status::Skipped => Verdict::Inconclusive,
            Status::Passed => Verdict::Met,
        };
        for &req in r.case.reqs {
            out.entry(req)
                .and_modify(|cur| {
                    *cur = match (*cur, v) {
                        (Verdict::Violated, _) | (_, Verdict::Violated) => Verdict::Violated,
                        (Verdict::Met, _) | (_, Verdict::Met) => Verdict::Met,
                        _ => Verdict::Inconclusive,
                    }
                })
                .or_insert(v);
        }
    }
    out
}

/// Context recorded in a report.
#[derive(Debug, Clone, Default)]
pub struct Meta {
    /// Tool name and version.
    pub tool: String,
    /// How the broker was reached (`127.0.0.1:1883`, `spawn: mosquitto -p {port}`).
    pub target: String,
    /// Unix time the run started, in seconds.
    pub started_unix: u64,
    /// Wall-clock duration.
    pub duration: Duration,
    /// Patterns that selected the cases.
    pub selection: Vec<String>,
}

/// Escapes `s` as a JSON string (with quotes).
#[must_use]
pub fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_list(items: impl IntoIterator<Item = String>) -> String {
    let items: Vec<String> = items.into_iter().collect();
    format!("[{}]", items.join(", "))
}

/// The run as a JSON document: metadata, summary, every case result and the
/// verdict for every cited requirement.
#[must_use]
pub fn json(meta: &Meta, results: &[CaseResult]) -> String {
    let s = Summary::of(results);
    let cat = spec::catalogue();
    let mut out = String::from("{\n");
    let _ = writeln!(out, "  \"tool\": {},", json_string(&meta.tool));
    let _ = writeln!(out, "  \"target\": {},", json_string(&meta.target));
    let _ = writeln!(out, "  \"started_unix\": {},", meta.started_unix);
    let _ = writeln!(out, "  \"duration_ms\": {},", meta.duration.as_millis());
    let _ = writeln!(
        out,
        "  \"selection\": {},",
        json_list(meta.selection.iter().map(|p| json_string(p)))
    );
    let _ = writeln!(
        out,
        "  \"summary\": {{\"total\": {}, \"passed\": {}, \"failed\": {}, \"skipped\": {}, \"timed_out\": {}}},",
        s.total, s.passed, s.failed, s.skipped, s.timed_out
    );
    out.push_str("  \"cases\": [\n");
    let cases: Vec<String> = results
        .iter()
        .map(|r| {
            format!(
                "    {{\"id\": {}, \"version\": {}, \"area\": {}, \"strict\": {}, \"status\": {}, \"duration_ms\": {}, \"message\": {}, \"notes\": {}, \"requirements\": {}}}",
                json_string(r.case.id),
                json_string(spec_label(r.case.version)),
                json_string(r.case.area),
                r.case.strict,
                json_string(r.status.label()),
                r.duration.as_millis(),
                r.message.as_deref().map_or_else(|| "null".to_owned(), json_string),
                json_list(r.notes.iter().map(|n| json_string(n))),
                json_list(r.case.reqs.iter().map(|q| json_string(q))),
            )
        })
        .collect();
    out.push_str(&cases.join(",\n"));
    out.push_str("\n  ],\n  \"requirements\": [\n");
    let reqs: Vec<String> = verdicts(results)
        .into_iter()
        .map(|(key, v)| {
            let (level, summary, url) = cat
                .get(key)
                .map_or(("", "", ""), |r| (r.level, r.summary, r.url));
            format!(
                "    {{\"id\": {}, \"verdict\": {}, \"level\": {}, \"summary\": {}, \"url\": {}}}",
                json_string(key),
                json_string(v.label()),
                json_string(level),
                json_string(summary),
                json_string(url),
            )
        })
        .collect();
    out.push_str(&reqs.join(",\n"));
    out.push_str("\n  ]\n}\n");
    out
}

/// Escapes `s` for XML text and attribute values.
#[must_use]
pub fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            // Characters XML 1.0 cannot carry.
            c if u32::from(c) < 0x20 && !matches!(c, '\n' | '\r' | '\t') => out.push('?'),
            c => out.push(c),
        }
    }
    out
}

/// The run as `JUnit` XML: one `<testsuite>` per version/area.
#[must_use]
pub fn junit(meta: &Meta, results: &[CaseResult]) -> String {
    let mut suites: BTreeMap<String, Vec<&CaseResult>> = BTreeMap::new();
    for r in results {
        let suite = r.case.id.rsplit_once('/').map_or(r.case.id, |(s, _)| s);
        suites.entry(suite.to_owned()).or_default().push(r);
    }
    let s = Summary::of(results);
    let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let _ = writeln!(
        out,
        "<testsuites name=\"{}\" tests=\"{}\" failures=\"{}\" skipped=\"{}\" time=\"{:.3}\">",
        xml_escape(&meta.tool),
        s.total,
        s.failed + s.timed_out,
        s.skipped,
        meta.duration.as_secs_f64()
    );
    for (name, rs) in suites {
        let ss = Summary::of(&rs.iter().map(|r| (*r).clone()).collect::<Vec<_>>());
        let time: f64 = rs.iter().map(|r| r.duration.as_secs_f64()).sum();
        let _ = writeln!(
            out,
            "  <testsuite name=\"{}\" tests=\"{}\" failures=\"{}\" skipped=\"{}\" time=\"{time:.3}\">",
            xml_escape(&name),
            ss.total,
            ss.failed + ss.timed_out,
            ss.skipped
        );
        for r in rs {
            let _ = write!(
                out,
                "    <testcase classname=\"{}\" name=\"{}\" time=\"{:.3}\"",
                xml_escape(&name),
                xml_escape(r.case.name),
                r.duration.as_secs_f64()
            );
            let msg = r.message.as_deref().unwrap_or("");
            let first = msg.lines().next().unwrap_or("");
            match r.status {
                Status::Passed => out.push_str(" />\n"),
                Status::Skipped => {
                    let _ = writeln!(
                        out,
                        ">\n      <skipped message=\"{}\" />\n    </testcase>",
                        xml_escape(first)
                    );
                }
                Status::Failed | Status::TimedOut => {
                    let _ = writeln!(
                        out,
                        ">\n      <failure type=\"{}\" message=\"{}\">{}\nrequirements: {}</failure>\n    </testcase>",
                        r.status.label(),
                        xml_escape(first),
                        xml_escape(msg),
                        xml_escape(&r.case.reqs.join(", "))
                    );
                }
            }
        }
        out.push_str("  </testsuite>\n");
    }
    out.push_str("</testsuites>\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry;

    fn result(id: &str, status: Status, message: Option<&str>) -> CaseResult {
        CaseResult {
            case: registry::find(id).unwrap(),
            status,
            message: message.map(str::to_owned),
            notes: vec!["n\"1".into()],
            duration: Duration::from_millis(12),
        }
    }

    fn sample() -> Vec<CaseResult> {
        let cases: Vec<&str> = registry::cases()
            .iter()
            .filter(|c| c.reqs.contains(&"5.0/MQTT-3.1.2-2"))
            .map(|c| c.id)
            .take(2)
            .collect();
        vec![
            result(cases[0], Status::Passed, None),
            result(
                cases.get(1).copied().unwrap_or(cases[0]),
                Status::Failed,
                Some("boom <&>\nline 2"),
            ),
            result(
                registry::cases()[0].id,
                Status::Skipped,
                Some("not offered"),
            ),
        ]
    }

    #[test]
    fn summary_counts() {
        let s = Summary::of(&sample());
        assert_eq!(
            (s.total, s.passed, s.failed, s.skipped, s.timed_out),
            (3, 1, 1, 1, 0)
        );
        assert!(!s.ok());
    }

    #[test]
    fn a_failure_anywhere_violates_the_requirement() {
        let v = verdicts(&sample());
        assert_eq!(v.get("5.0/MQTT-3.1.2-2"), Some(&Verdict::Violated));
        let only_pass = verdicts(&sample()[..1]);
        assert_eq!(only_pass.get("5.0/MQTT-3.1.2-2"), Some(&Verdict::Met));
    }

    #[test]
    fn skip_then_pass_is_met_and_skip_alone_is_inconclusive() {
        let id = registry::cases()
            .iter()
            .find(|c| !c.reqs.is_empty())
            .unwrap()
            .id;
        let skip = result(id, Status::Skipped, Some("x"));
        let pass = result(id, Status::Passed, None);
        let req = registry::find(id).unwrap().reqs[0];
        assert_eq!(
            verdicts(std::slice::from_ref(&skip)).get(req),
            Some(&Verdict::Inconclusive)
        );
        assert_eq!(verdicts(&[skip, pass]).get(req), Some(&Verdict::Met));
    }

    #[test]
    fn json_is_escaped_and_complete() {
        let doc = json(&Meta::default(), &sample());
        assert!(doc.contains(r#""message": "boom <&>\nline 2""#), "{doc}");
        assert!(doc.contains(r#""notes": ["n\"1"]"#), "{doc}");
        assert!(doc.contains(r#""verdict": "violated""#), "{doc}");
        assert_eq!(json_string("a\u{1}b"), "\"a\\u0001b\"");
    }

    #[test]
    fn junit_marks_failures_and_skips() {
        let xml = junit(&Meta::default(), &sample());
        assert!(
            xml.contains("<failure type=\"failed\" message=\"boom &lt;&amp;&gt;\""),
            "{xml}"
        );
        assert!(xml.contains("<skipped message=\"not offered\" />"), "{xml}");
        assert!(xml.contains("failures=\"1\""), "{xml}");
    }
}
