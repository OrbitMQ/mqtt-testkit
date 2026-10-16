// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! `browse`: an interactive, line-based navigator over versions, areas,
//! cases and requirements, which can also run a case or an area.

use std::io::{BufRead as _, Write as _};
use std::process::ExitCode;
use std::time::Duration;

use mqtt_testkit::broker::Target;
use mqtt_testkit::registry::{self, Case, area_title, spec_label};
use mqtt_testkit::report::Summary;
use mqtt_testkit::runner;
use mqtt_testkit::{Version, interop, spec};

use crate::args::{Args, flag};
use crate::{
    BROKER_OPTS, Verbosity, coverage_table, describe, execute, preflight, render_case,
    render_requirement, search, target_from, term,
};

/// Where the navigator is.
#[derive(Debug, Clone)]
enum Screen {
    Home,
    Version(Version),
    Area(Version, &'static str),
    Interop,
    Case(&'static Case),
    Coverage,
    Text(String),
}

struct State {
    target: Option<Target>,
    stack: Vec<Screen>,
}

pub(crate) fn run(raw: &[String]) -> Result<ExitCode, String> {
    let mut opts = BROKER_OPTS.to_vec();
    opts.push(flag("no-color", None));
    let args = Args::parse(raw, &opts)?;
    term::init(args.flag("no-color"));
    let mut state = State {
        target: target_from(&args)?,
        stack: vec![Screen::Home],
    };
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    while let Some(screen) = state.stack.last().cloned() {
        let choices = render(&screen, &state);
        print!("\n{} ", term::bold(">"));
        let _ = std::io::stdout().flush();
        let Some(Ok(line)) = lines.next() else {
            println!();
            break;
        };
        let input = line.trim();
        match input {
            "q" | "quit" | "exit" => break,
            "" => continue,
            "b" | ".." | "back" => {
                if state.stack.len() > 1 {
                    state.stack.pop();
                }
                continue;
            }
            "h" | "home" => {
                state.stack.truncate(1);
                continue;
            }
            "?" | "help" => {
                println!(
                    "{}",
                    term::dim(
                        "number = open · b = back · h = home · / TEXT = search · r = run (case/area/version) \
                         · broker HOST:PORT = set broker · q = quit"
                    )
                );
                continue;
            }
            _ => {}
        }
        if let Some(q) = input.strip_prefix('/') {
            state.stack.push(Screen::Text(crate::show(q)));
            continue;
        }
        if let Some(addr) = input.strip_prefix("broker ") {
            state.target = Some(Target::Address(addr.trim().to_owned()));
            println!("broker set to {}", addr.trim());
            continue;
        }
        if input == "r" {
            run_screen(&screen, &mut state, &mut lines);
            continue;
        }
        if let Ok(n) = input.parse::<usize>()
            && let Some(next) = n.checked_sub(1).and_then(|i| choices.get(i))
        {
            state.stack.push(next.clone());
            continue;
        }
        // Anything else: a case id, a requirement id or search text.
        if let Some(c) = registry::find(input) {
            state.stack.push(Screen::Case(c));
        } else if let Some(r) = spec::catalogue().get(input) {
            state.stack.push(Screen::Text(render_requirement(r)));
        } else {
            state.stack.push(Screen::Text(search(input)));
        }
    }
    Ok(ExitCode::SUCCESS)
}

#[allow(clippy::too_many_lines, reason = "one arm per screen")]
/// Prints `screen` and returns the screens its numbered entries lead to.
fn render(screen: &Screen, state: &State) -> Vec<Screen> {
    let mut choices = Vec::new();
    println!();
    match screen {
        Screen::Home => {
            let cat = spec::catalogue();
            println!(
                "{} {}",
                term::bold("MQTT conformance suite"),
                term::dim(&format!(
                    "· {} cases · {} requirements",
                    registry::cases().len(),
                    cat.requirements.len()
                ))
            );
            println!(
                "{}",
                term::dim(&format!(
                    "broker: {}",
                    state
                        .target
                        .as_ref()
                        .map_or_else(|| "not set (type: broker HOST:PORT)".to_owned(), describe)
                ))
            );
            for v in registry::VERSIONS {
                let n = registry::cases().iter().filter(|c| c.version == v).count();
                let title = cat.source(v).map_or("", |s| s.title);
                choices.push(Screen::Version(v));
                println!(
                    "  {}  MQTT {:<6} {:>4} cases  {}",
                    term::cyan(&choices.len().to_string()),
                    spec_label(v),
                    n,
                    term::dim(title)
                );
            }
            choices.push(Screen::Interop);
            println!(
                "  {}  Interoperability {:>3} cases  {}",
                term::cyan(&choices.len().to_string()),
                interop::CASES.len(),
                term::dim("client workflows, mosquitto clients")
            );
            choices.push(Screen::Coverage);
            println!(
                "  {}  Requirement coverage",
                term::cyan(&choices.len().to_string())
            );
            println!(
                "\n{}",
                term::dim(
                    "number = open · / TEXT = search · broker HOST:PORT · ? = help · q = quit"
                )
            );
        }
        Screen::Version(v) => {
            println!("{}", term::bold(&format!("MQTT {}", spec_label(*v))));
            for (area, n) in registry::areas(*v) {
                choices.push(Screen::Area(*v, area));
                println!(
                    "  {:>2}  {:<44} {:>3} cases",
                    term::cyan(&choices.len().to_string()),
                    area_title(area),
                    n
                );
            }
            println!(
                "\n{}",
                term::dim("number = open · r = run every case of this version · b = back")
            );
        }
        Screen::Area(v, area) => {
            println!(
                "{}",
                term::bold(&format!("MQTT {} · {}", spec_label(*v), area_title(area)))
            );
            for c in registry::cases()
                .iter()
                .filter(|c| c.version == *v && c.area == *area)
            {
                choices.push(Screen::Case(c));
                println!(
                    "  {:>2}  {}  {}",
                    term::cyan(&choices.len().to_string()),
                    c.title(),
                    term::dim(&format!("[{}]", c.reqs.len()))
                );
            }
            println!(
                "\n{}",
                term::dim("number = open · r = run this area · b = back")
            );
        }
        Screen::Interop => {
            println!("{}", term::bold("Interoperability"));
            for c in interop::CASES {
                choices.push(Screen::Case(c));
                println!("  {:>2}  {}", term::cyan(&choices.len().to_string()), c.id);
            }
            println!("\n{}", term::dim("number = open · r = run all · b = back"));
        }
        Screen::Case(c) => {
            print!("{}", render_case(c));
            println!("\n{}", term::dim("r = run this case · b = back"));
        }
        Screen::Coverage => {
            print!("{}", coverage_table(true));
            println!(
                "\n{}",
                term::dim("b = back · type a requirement id to open it")
            );
        }
        Screen::Text(text) => {
            print!("{text}");
            println!(
                "\n{}",
                term::dim("b = back · type a case or requirement id to open it")
            );
        }
    }
    choices
}

/// Runs the cases behind `screen`, asking for a broker first if needed.
fn run_screen(
    screen: &Screen,
    state: &mut State,
    lines: &mut impl Iterator<Item = std::io::Result<String>>,
) {
    let cases: Vec<&'static Case> = match screen {
        Screen::Version(v) => registry::cases()
            .iter()
            .filter(|c| c.version == *v)
            .collect(),
        Screen::Area(v, a) => registry::cases()
            .iter()
            .filter(|c| c.version == *v && c.area == *a)
            .collect(),
        Screen::Interop => interop::CASES.iter().collect(),
        Screen::Case(c) => vec![*c],
        _ => {
            println!(
                "{}",
                term::dim("nothing to run here: open a version, an area or a case")
            );
            return;
        }
    };
    if state.target.is_none() {
        print!("broker HOST:PORT: ");
        let _ = std::io::stdout().flush();
        match lines.next() {
            Some(Ok(l)) if !l.trim().is_empty() => {
                state.target = Some(Target::Address(l.trim().to_owned()));
            }
            _ => return,
        }
    }
    let Some(target) = state.target.clone() else {
        return;
    };
    if let Err(e) = preflight(&target) {
        println!("{} {e}", term::red("broker:"));
        return;
    }
    let interop = cases.iter().any(|c| registry::is_interop(c));
    let options = runner::Options {
        jobs: if interop { 1 } else { runner::default_jobs() },
        case_timeout: Duration::from_secs(if interop { 3600 } else { 120 }),
        fail_fast: false,
    };
    println!(
        "running {} case(s) against {}",
        cases.len(),
        describe(&target)
    );
    let results = execute(&cases, &options, Verbosity::Verbose, false);
    let s = Summary::of(&results);
    println!(
        "\n{}",
        if s.ok() {
            term::green(&format!("✓ {} passed, {} skipped", s.passed, s.skipped))
        } else {
            term::red(&format!(
                "✗ {} failed, {} passed, {} skipped",
                s.failed + s.timed_out,
                s.passed,
                s.skipped
            ))
        }
    );
}
