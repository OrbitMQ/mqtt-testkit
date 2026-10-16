// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Generates the case registry from `src/suite/<version>/<area>.rs`.
//!
//! A case is a top-level `pub fn name()` in an area file (any file other
//! than `mod.rs`). Its requirements are the `// req: <version>/<ID>`
//! markers in its body plus those in same-file helper functions it calls
//! (transitively). Its description is its `///` doc comment, if any; a doc
//! line starting with `Strict:` puts the case in the strict tier.
//!
//! Outputs, in `OUT_DIR`:
//! - `registry.rs`: the `CASES` table, included by `src/registry.rs`;
//! - `cargo_tests_<version dir>.rs`: one `#[test]` per case, included by
//!   `tests/<version dir>.rs` so `cargo test --test v5` keeps working.
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "build script: aborting the build is how it reports an error"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

/// `(directory under src/suite, spec version, `Version` variant)`.
const VERSIONS: [(&str, &str, &str); 3] = [
    ("v3_1", "3.1", "V31"),
    ("v3_1_1", "3.1.1", "V311"),
    ("v5", "5.0", "V5"),
];

/// Areas in reading order (connection first, newest features last); other
/// area files follow alphabetically.
const AREA_ORDER: [&str; 14] = [
    "connect",
    "malformed",
    "lifecycle",
    "pubsub",
    "retain",
    "topics",
    "persistence",
    "will",
    "options",
    "properties",
    "flow",
    "shared",
    "websocket",
    "interop",
];

struct Function {
    name: String,
    public: bool,
    line: usize,
    doc: Vec<String>,
    body: String,
    reqs: Vec<String>,
}

fn main() {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    println!("cargo:rerun-if-changed=src/suite");

    let mut registry =
        String::from("/// Every case, ordered by version, area and source position.\n");
    registry.push_str("pub static CASES: &[Case] = &[\n");
    for (dir, version, variant) in VERSIONS {
        let mut tests = String::new();
        let area_dir = root.join("src/suite").join(dir);
        let mut files: Vec<PathBuf> = fs::read_dir(&area_dir)
            .unwrap_or_else(|e| panic!("{}: {e}", area_dir.display()))
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "rs"))
            .filter(|p| p.file_name().is_some_and(|n| n != "mod.rs"))
            .collect();
        let rank = |p: &PathBuf| {
            let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            (
                AREA_ORDER
                    .iter()
                    .position(|a| *a == stem)
                    .unwrap_or(AREA_ORDER.len()),
                stem.to_owned(),
            )
        };
        files.sort_by_key(rank);
        for file in files {
            println!("cargo:rerun-if-changed={}", file.display());
            let area = file
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("file name")
                .to_owned();
            let source = fs::read_to_string(&file).expect("read area file");
            let functions = parse_functions(&source);
            let rel = format!("src/suite/{dir}/{area}.rs");
            for f in functions.iter().filter(|f| f.public) {
                let reqs = requirements(f, &functions);
                for r in &reqs {
                    assert!(
                        r.starts_with(&format!("{version}/")),
                        "{rel}:{}: case {} cites {r}, which is not a {version} requirement",
                        f.line,
                        f.name
                    );
                }
                let id = format!("{version}/{area}/{}", f.name);
                let _ = writeln!(
                    registry,
                    "    Case {{ id: {id:?}, version: Version::{variant}, area: {area:?}, name: {name:?}, \
                     doc: {doc:?}, reqs: &{reqs:?}, strict: {strict}, file: {rel:?}, line: {line}, \
                     run: crate::suite::{dir}::{area}::{name} }},",
                    name = f.name,
                    doc = f.doc.join("\n"),
                    strict = f.doc.iter().any(|d| d.starts_with("Strict:")),
                    line = f.line,
                );
                let _ = writeln!(
                    tests,
                    "#[test]\nfn {area}__{name}() {{\n    mqtt_testkit::registry::run_for_cargo_test({id:?});\n}}\n",
                    name = f.name,
                );
            }
        }
        fs::write(out.join(format!("cargo_tests_{dir}.rs")), tests).expect("write tests");
    }
    registry.push_str("];\n");
    fs::write(out.join("registry.rs"), registry).expect("write registry");
}

/// Top-level functions of `source` (lines starting with `fn` or `pub fn`;
/// the body ends at the first following line that is exactly `}`).
fn parse_functions(source: &str) -> Vec<Function> {
    let lines: Vec<&str> = source.lines().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        let (public, rest) = if let Some(r) = line.strip_prefix("pub fn ") {
            (true, r)
        } else if let Some(r) = line.strip_prefix("fn ") {
            (false, r)
        } else if let Some(r) = line
            .strip_prefix("pub(super) fn ")
            .or_else(|| line.strip_prefix("pub(crate) fn "))
        {
            (false, r)
        } else {
            i += 1;
            continue;
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        let mut doc = Vec::new();
        let mut j = i;
        while j > 0 {
            let prev = lines[j - 1].trim_start();
            if let Some(d) = prev.strip_prefix("///") {
                doc.insert(0, d.strip_prefix(' ').unwrap_or(d).to_owned());
            } else if !prev.starts_with("#[") {
                break;
            }
            j -= 1;
        }
        let start = i;
        let mut end = i;
        while end < lines.len() && lines[end] != "}" {
            end += 1;
        }
        let body = lines[start..end.min(lines.len())].join("\n");
        let reqs = body
            .lines()
            .filter_map(|l| l.trim_start().strip_prefix("// req:"))
            .map(|r| r.trim().to_owned())
            .collect();
        if public {
            assert!(
                rest[name.len()..].starts_with("()"),
                "line {}: case `{name}` must take no arguments",
                i + 1
            );
        }
        out.push(Function {
            name,
            public,
            line: i + 1,
            doc,
            body,
            reqs,
        });
        i = end + 1;
    }
    out
}

/// The case's own markers plus those of same-file helpers it calls,
/// transitively; first-seen order, no duplicates.
fn requirements(case: &Function, all: &[Function]) -> Vec<String> {
    let helpers: BTreeMap<&str, &Function> = all
        .iter()
        .filter(|f| !f.public)
        .map(|f| (f.name.as_str(), f))
        .collect();
    let mut seen_fns = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    let mut stack = vec![case];
    while let Some(f) = stack.pop() {
        if !seen_fns.insert(f.name.as_str()) {
            continue;
        }
        for r in &f.reqs {
            if seen.insert(r.clone()) {
                out.push(r.clone());
            }
        }
        for (name, helper) in &helpers {
            if calls(&f.body, name) {
                stack.push(helper);
            }
        }
    }
    out
}

/// Whether `body` calls `name` (`name(` not preceded by an identifier char,
/// `.` or `::`).
fn calls(body: &str, name: &str) -> bool {
    let pat = format!("{name}(");
    body.match_indices(&pat).any(|(at, _)| {
        let before = &body[..at];
        !(before.ends_with(|c: char| c.is_alphanumeric() || c == '_' || c == '.')
            || before.ends_with("fn ")
            || before.ends_with("::"))
    })
}
