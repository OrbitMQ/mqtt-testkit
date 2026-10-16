// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! The requirement catalogue: every normative statement of MQTT 3.1, 3.1.1
//! and 5.0, with a one-line paraphrase and a link into the specification.
//!
//! MQTT 3.1.1 and 5.0 statement ids come from the specifications' Appendix B
//! (conformance statements). MQTT 3.1 has no tagged statements; its `V31-*`
//! ids are derived from the 3.1 text. Ids are qualified by version
//! (`5.0/MQTT-3.1.2-1`) because 3.1.1 and 5.0 reuse tag numbers with
//! different meanings.
//!
//! The data is `spec/requirements.tsv`, embedded at build time.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::codec::Version;
use crate::registry::{self, Case, parse_spec};

const DATA: &str = include_str!("../spec/requirements.tsv");
const APPLICABILITY: &str = include_str!("../spec/applicability.tsv");

/// Why a server-binding requirement has no black-box case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Exemption {
    /// No externally observable server behaviour distinguishes compliance.
    NotBlackBox,
    /// Checkable only with a broker feature or setup the suite cannot
    /// provide generically.
    NeedsCapability,
    /// A MAY / SHOULD statement the suite deliberately does not assert.
    Informative,
}

impl Exemption {
    /// The label used in `spec/applicability.tsv`.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::NotBlackBox => "not-black-box",
            Self::NeedsCapability => "needs-capability",
            Self::Informative => "informative",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "not-black-box" => Some(Self::NotBlackBox),
            "needs-capability" => Some(Self::NeedsCapability),
            "informative" => Some(Self::Informative),
            _ => None,
        }
    }
}

/// One normative statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requirement {
    /// Version-qualified id: `5.0/MQTT-3.1.2-1`.
    pub key: String,
    /// Spec version.
    pub version: Version,
    /// Statement id within its version: `MQTT-3.1.2-1`.
    pub id: &'static str,
    /// Specification section, e.g. `3.1.2`.
    pub section: &'static str,
    /// Topic, e.g. `connect`, `publish`, `qos-delivery`.
    pub area: &'static str,
    /// `MUST`, `MUST NOT`, `SHOULD`, `SHOULD NOT` or `MAY`.
    pub level: &'static str,
    /// Who the statement binds: `server`, `client`, `both`, `sender` or `receiver`.
    pub actor: &'static str,
    /// Whether a server can violate it (so a broker test applies).
    pub server_binding: bool,
    /// One-line paraphrase.
    pub summary: &'static str,
    /// Link into the specification.
    pub url: &'static str,
    /// Why no black-box case checks it, if so (`spec/applicability.tsv`).
    pub exemption: Option<(Exemption, &'static str)>,
}

/// A specification document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    /// Spec version.
    pub version: Version,
    /// Document title.
    pub title: &'static str,
    /// Document URL.
    pub url: &'static str,
}

/// The parsed catalogue.
#[derive(Debug)]
pub struct Catalogue {
    /// Every requirement, in specification order per version.
    pub requirements: Vec<Requirement>,
    /// The specification documents.
    pub sources: Vec<Source>,
    index: BTreeMap<String, usize>,
}

impl Catalogue {
    /// The requirement with this version-qualified key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Requirement> {
        self.index.get(key).and_then(|&i| self.requirements.get(i))
    }

    /// The requirements of `version`.
    pub fn of(&self, version: Version) -> impl Iterator<Item = &Requirement> {
        self.requirements
            .iter()
            .filter(move |r| r.version == version)
    }

    /// The source document of `version`.
    #[must_use]
    pub fn source(&self, version: Version) -> Option<&Source> {
        self.sources.iter().find(|s| s.version == version)
    }
}

/// The embedded catalogue.
#[must_use]
pub fn catalogue() -> &'static Catalogue {
    static CATALOGUE: OnceLock<Catalogue> = OnceLock::new();
    CATALOGUE.get_or_init(|| parse(DATA))
}

/// Parses the TSV format written by `scripts/export-requirements.py`.
/// Malformed rows are ignored (a unit test checks there are none).
fn parse(data: &'static str) -> Catalogue {
    let mut requirements = Vec::new();
    let mut sources = Vec::new();
    for line in data.lines() {
        let fields: Vec<&'static str> = line.split('\t').collect();
        if let Some(source) = line.strip_prefix("# source\t") {
            let f: Vec<&'static str> = source.split('\t').collect();
            if let [v, title, url] = f[..]
                && let Some(version) = parse_spec(v)
            {
                sources.push(Source {
                    version,
                    title,
                    url,
                });
            }
            continue;
        }
        if line.starts_with('#') || line.starts_with("version\t") {
            continue;
        }
        let [v, id, section, area, level, actor, binding, summary, url] = fields[..] else {
            continue;
        };
        let Some(version) = parse_spec(v) else {
            continue;
        };
        requirements.push(Requirement {
            key: format!("{v}/{id}"),
            version,
            id,
            section,
            area,
            level,
            actor,
            server_binding: binding == "1",
            summary,
            url,
            exemption: None,
        });
    }
    for line in APPLICABILITY.lines() {
        if line.starts_with('#') || line.starts_with("requirement\t") {
            continue;
        }
        if let [key, category, reason] = line.split('\t').collect::<Vec<_>>()[..]
            && let Some(category) = Exemption::parse(category)
            && let Some(r) = requirements.iter_mut().find(|r| r.key == key)
        {
            r.exemption = Some((category, reason));
        }
    }
    let index = requirements
        .iter()
        .enumerate()
        .map(|(i, r)| (r.key.clone(), i))
        .collect();
    Catalogue {
        requirements,
        sources,
        index,
    }
}

/// The cases citing `key`.
#[must_use]
pub fn cases_for(key: &str) -> Vec<&'static Case> {
    registry::all().filter(|c| c.reqs.contains(&key)).collect()
}

/// Requirement coverage of one version by the case registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Coverage {
    /// Spec version.
    pub version: Version,
    /// All requirements.
    pub total: usize,
    /// Server-binding requirements (the ones a broker can violate).
    pub server_binding: usize,
    /// Server-binding requirements cited by at least one case.
    pub covered: usize,
    /// Server-binding requirements without a case, with a documented
    /// exemption.
    pub exempt: usize,
}

/// Coverage per version, oldest first.
#[must_use]
pub fn coverage() -> Vec<Coverage> {
    let cat = catalogue();
    registry::VERSIONS
        .iter()
        .map(|&version| {
            let reqs: Vec<&Requirement> = cat.of(version).collect();
            let binding: Vec<&&Requirement> = reqs.iter().filter(|r| r.server_binding).collect();
            let covered = binding
                .iter()
                .filter(|r| !cases_for(&r.key).is_empty())
                .count();
            let exempt = binding
                .iter()
                .filter(|r| r.exemption.is_some() && cases_for(&r.key).is_empty())
                .count();
            Coverage {
                version,
                total: reqs.len(),
                server_binding: binding.len(),
                covered,
                exempt,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogue_has_every_version_and_no_malformed_rows() {
        let cat = catalogue();
        let rows = DATA
            .lines()
            .filter(|l| !l.starts_with('#') && !l.starts_with("version\t") && !l.is_empty())
            .count();
        assert_eq!(cat.requirements.len(), rows);
        assert_eq!(cat.of(Version::V31).count(), 101);
        assert_eq!(cat.of(Version::V311).count(), 141);
        assert_eq!(cat.of(Version::V5).count(), 251);
        assert_eq!(cat.sources.len(), 3);
        for r in &cat.requirements {
            assert!(
                ["MUST", "MUST NOT", "SHOULD", "SHOULD NOT", "MAY"].contains(&r.level),
                "{r:?}"
            );
            assert!(r.url.starts_with("http"), "{r:?}");
        }
    }

    #[test]
    fn every_cited_requirement_exists() {
        let cat = catalogue();
        for case in registry::all() {
            for r in case.reqs {
                assert!(
                    cat.get(r).is_some(),
                    "{} cites unknown requirement {r}",
                    case.id
                );
            }
        }
    }

    #[test]
    fn applicability_rows_are_well_formed_and_not_stale() {
        let cat = catalogue();
        for line in APPLICABILITY.lines() {
            if line.starts_with('#') || line.starts_with("requirement\t") || line.is_empty() {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            let [key, category, reason] = fields[..] else {
                panic!("malformed applicability row {line:?}");
            };
            let r = cat
                .get(key)
                .unwrap_or_else(|| panic!("unknown requirement {key}"));
            assert!(
                r.server_binding,
                "{key} is not server-binding; no exemption needed"
            );
            assert!(
                Exemption::parse(category).is_some(),
                "{key}: bad category {category}"
            );
            assert!(reason.len() > 10, "{key}: give a reason");
            assert!(
                cases_for(key).is_empty(),
                "{key} is exempted but cited by {:?}; remove the exemption",
                cases_for(key).iter().map(|c| c.id).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn every_server_binding_requirement_is_checked_or_exempt() {
        let open: Vec<&str> = catalogue()
            .requirements
            .iter()
            .filter(|r| r.server_binding && r.exemption.is_none() && cases_for(&r.key).is_empty())
            .map(|r| r.key.as_str())
            .collect();
        assert!(
            open.is_empty(),
            "server-binding requirements with neither a case nor an entry in \
             spec/applicability.tsv: {open:?}"
        );
    }

    #[test]
    fn coverage_counts_are_consistent() {
        for c in coverage() {
            assert!(
                c.covered <= c.server_binding && c.server_binding <= c.total,
                "{c:?}"
            );
            assert!(c.covered > 0, "{c:?}");
        }
    }
}
