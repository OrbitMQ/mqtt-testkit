// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! `mqtt-testkit`: requirement-mapped MQTT 3.1 / 3.1.1 / 5.0
//! conformance and interoperability suite for any broker.

mod args;
mod browse;
mod term;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::{TcpStream, ToSocketAddrs};
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mqtt_testkit::broker::{self, Target};
use mqtt_testkit::codec::{PropValue, prop};
use mqtt_testkit::registry::{self, Case, area_title, spec_label};
use mqtt_testkit::report::{self, Meta, Summary, Verdict};
use mqtt_testkit::runner::{self, CaseResult, Event, Status};
use mqtt_testkit::{Broker, Connect, Version, spec, unique_id, ws};

use args::{Args, Opt, flag, value};

const TOOL: &str = concat!("mqtt-testkit ", env!("CARGO_PKG_VERSION"));

const USAGE: &str = "\
mqtt-testkit: MQTT 3.1 / 3.1.1 / 5.0 conformance and interoperability suite

Usage: mqtt-testkit <command> [options] [patterns]

Commands:
  run [PATTERN...]      Run cases against a broker (default: every conformance case)
  list [PATTERN...]     List cases
  show <CASE|REQ>       Explain a case or a requirement
  reqs [PATTERN...]     List requirements and the cases that check them
  coverage              Requirement coverage of the suite, per version
  probe                 Connect with each protocol version and show what the broker announces
  interop [scenarios|mosquitto]
                        Run the interoperability suites (independent client implementations)
  browse                Interactive navigator (versions, areas, cases, requirements)
  help [COMMAND]        Show help

Patterns select cases by id glob (5.0/connect/*), id prefix (3.1.1, 5.0/will) or
requirement id (MQTT-3.3.1-4, 5.0/MQTT-4.7.*, V31-3.1-4).

Run 'mqtt-testkit help <command>' for the options of a command.
";

const BROKER_HELP: &str = "\
Broker:
  -b, --broker HOST:PORT   Test a running broker (or set MQTT_TESTKIT_BROKER)
  -s, --spawn 'CMD ARGS'   Start a fresh broker for every case on a free port; {port} and {addr}
                           are substituted, and ORBITMQ_LISTEN=127.0.0.1:<port> is set
                           (or set MQTT_TESTKIT_BIN and MQTT_TESTKIT_BIN_ARGS)
      --ws URL             Also test MQTT over WebSocket at URL (ws://host:port/path, no TLS;
                           or set MQTT_TESTKIT_WS). WebSocket cases skip without it
  The broker must accept anonymous plain-TCP connections.
";

const RUN_HELP: &str = "\
Usage: mqtt-testkit run [options] [PATTERN...]

Runs the selected cases (every conformance case by default) and reports, for every requirement
they check, whether the broker met or violated it. Exit status: 0 no failures, 1 failures,
2 usage or broker error.

Options:
  -x, --exclude PATTERN    Leave out matching cases (repeatable, comma-separated)
  -j, --jobs N             Cases run in parallel (default: CPUs, at most 8)
      --timeout-ms N       Wait for an expected broker reaction (default 2000)
      --quiet-ms N         Wait before concluding that nothing happens (default 1000)
      --case-timeout S     Abandon a case after S seconds (default 120)
      --fail-fast          Stop after the first failure
      --strict             Also run strict-tier cases: the server must detect and reject a
                           client's violation of a client-side rule (the spec requires rejection
                           only once detected, so a compliant broker may fail them)
      --format FMT         text (default), json or junit, on stdout
      --json FILE          Also write a JSON report to FILE
      --junit FILE         Also write a JUnit XML report to FILE
  -v, --verbose            One line per case, with skip reasons and notes
  -q, --quiet              Only the summary and failures
      --no-color           No ANSI colours (also NO_COLOR=1)
";

const BROKER_OPTS: [Opt; 3] = [
    value("broker", Some('b')),
    value("spawn", Some('s')),
    value("ws", None),
];

fn main() -> ExitCode {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let Some((command, rest)) = raw.split_first() else {
        print!("{USAGE}");
        return ExitCode::from(2);
    };
    let result = match command.as_str() {
        "run" => cmd_run(rest, &[]),
        "interop" => cmd_interop(rest),
        "list" | "ls" => cmd_list(rest),
        "show" => cmd_show(rest),
        "reqs" | "requirements" => cmd_reqs(rest),
        "coverage" => cmd_coverage(rest),
        "probe" => cmd_probe(rest),
        "browse" => browse::run(rest),
        "help" | "-h" | "--help" => {
            help(rest.first().map(String::as_str));
            Ok(ExitCode::SUCCESS)
        }
        "version" | "-V" | "--version" => {
            println!("{TOOL}");
            Ok(ExitCode::SUCCESS)
        }
        other => Err(format!(
            "unknown command {other:?} (try 'mqtt-testkit help')"
        )),
    };
    broker::kill_spawned();
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

fn help(command: Option<&str>) {
    match command {
        Some("run") => print!("{RUN_HELP}\n{BROKER_HELP}"),
        Some("interop") => print!(
            "Usage: mqtt-testkit interop [scenarios|mosquitto] [run options]\n\n\
             Runs the interoperability checks:\n\
             - scenarios: end-to-end client workflows for 3.1.1 and 5.0 (the `interop` area of each\n\
               version; they also run with every `run`). Set MQTT_TESTKIT_INTEROP_DENY_FILTER to a\n\
               filter your broker refuses to run the policy scenario.\n\
             - mosquitto: the mosquitto_pub / mosquitto_sub command-line clients, -V mqttv31/311/5\n\
               (skipped when they are not installed).\n\n\
             Accepts every 'run' option.\n\n{BROKER_HELP}"
        ),
        Some("list" | "ls") => print!(
            "Usage: mqtt-testkit list [PATTERN...] [-l|--long] [-x PATTERN]\n\n\
             Lists cases, grouped by version and area. --long adds the requirements of each case.\n\
             Interop cases are listed with the pattern 'interop'.\n"
        ),
        Some("show") => print!(
            "Usage: mqtt-testkit show <CASE-ID | REQUIREMENT | TEXT>\n\n\
             A case id shows its description, source and requirements. A requirement id\n\
             (5.0/MQTT-3.1.2-1, or unqualified MQTT-3.1.2-1) shows its text and the cases checking\n\
             it. Anything else searches case ids and requirement summaries.\n"
        ),
        Some("reqs" | "requirements") => print!(
            "Usage: mqtt-testkit reqs [PATTERN...] [options]\n\n\
             Lists requirements with their level and how many cases check them.\n\n\
             Options:\n\
             \x20     --version V      3.1, 3.1.1 or 5.0\n\
             \x20     --area AREA      e.g. connect, publish, qos-delivery\n\
             \x20     --uncovered      Only server-binding requirements no case checks\n\
             \x20     --all            Include requirements that bind only clients\n"
        ),
        Some("coverage") => print!(
            "Usage: mqtt-testkit coverage [-v]\n\n\
             Shows, per version, how many server-binding requirements at least one case checks.\n\
             -v lists the ones that are not checked.\n"
        ),
        Some("probe") => print!(
            "Usage: mqtt-testkit probe [broker options]\n\n\
             Connects once with MQTT 3.1, 3.1.1 and 5.0 and prints the CONNACK, including the 5.0\n\
             capabilities the broker announces.\n\n{BROKER_HELP}"
        ),
        Some("browse") => print!(
            "Usage: mqtt-testkit browse [broker options]\n\n\
             Interactive navigator: pick a version, an area and a case; read its requirements;\n\
             run a case or a whole area against the broker.\n\n{BROKER_HELP}"
        ),
        _ => print!("{USAGE}"),
    }
}

// ------------------------------------------------------------- broker ---

/// The broker target from `--broker` / `--spawn`, else the environment.
/// Also applies `--ws URL` (the WebSocket endpoint) when given.
pub(crate) fn target_from(args: &Args) -> Result<Option<Target>, String> {
    if let Some(url) = args.value("ws") {
        ws::WsUrl::parse(url).map_err(|e| format!("--ws: {e}"))?;
        ws::set_endpoint(Some(url.trim().to_owned()));
    }
    match (args.value("broker"), args.value("spawn")) {
        (Some(_), Some(_)) => Err("give either --broker or --spawn, not both".into()),
        (Some(addr), None) => Ok(Some(Target::Address(addr.to_owned()))),
        (None, Some(cmd)) => {
            let cmd = cmd.trim();
            let (program, args) = cmd.split_once(char::is_whitespace).unwrap_or((cmd, ""));
            if program.is_empty() {
                return Err("--spawn needs a command".into());
            }
            Ok(Some(Target::Spawn {
                program: program.to_owned(),
                args: args.trim().to_owned(),
            }))
        }
        (None, None) => {
            if let Some(bin) = mqtt_testkit::env_var("MQTT_TESTKIT_BIN") {
                return Ok(Some(Target::Spawn {
                    program: bin,
                    args: mqtt_testkit::env_var("MQTT_TESTKIT_BIN_ARGS").unwrap_or_default(),
                }));
            }
            Ok(mqtt_testkit::env_var("MQTT_TESTKIT_BROKER")
                .filter(|a| !a.is_empty())
                .map(Target::Address))
        }
    }
}

/// Human description of a target.
pub(crate) fn describe(target: &Target) -> String {
    match target {
        Target::Address(a) => a.clone(),
        Target::Spawn { program, args } if args.is_empty() => format!("spawn: {program}"),
        Target::Spawn { program, args } => format!("spawn: {program} {args}"),
    }
}

/// Checks that the target is reachable (or can be started) before a run.
pub(crate) fn preflight(target: &Target) -> Result<(), String> {
    broker::set_target(Some(target.clone()));
    match target {
        Target::Address(addr) => {
            let addrs: Vec<_> = addr
                .to_socket_addrs()
                .map_err(|e| format!("cannot resolve {addr}: {e}"))?
                .collect();
            let reachable = addrs
                .iter()
                .any(|a| TcpStream::connect_timeout(a, Duration::from_secs(3)).is_ok());
            if reachable {
                Ok(())
            } else {
                Err(format!("nothing accepts connections at {addr}"))
            }
        }
        Target::Spawn { .. } => match Broker::from_env() {
            Ok(Some(b)) => {
                drop(b);
                Ok(())
            }
            Ok(None) => Err("no broker".into()),
            Err(e) => Err(e.to_string()),
        },
    }
}

// ---------------------------------------------------------------- run ---

const RUN_OPTS: [Opt; 16] = [
    flag("strict", None),
    value("broker", Some('b')),
    value("spawn", Some('s')),
    value("ws", None),
    value("exclude", Some('x')),
    value("jobs", Some('j')),
    value("timeout-ms", None),
    value("quiet-ms", None),
    value("case-timeout", None),
    flag("fail-fast", None),
    value("format", None),
    value("json", None),
    value("junit", None),
    flag("verbose", Some('v')),
    flag("quiet", Some('q')),
    flag("no-color", None),
];

/// How results are printed while a run is in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verbosity {
    Quiet,
    Normal,
    Verbose,
}

fn cmd_run(raw: &[String], implicit: &[String]) -> Result<ExitCode, String> {
    let args = Args::parse(raw, &RUN_OPTS)?;
    term::init(args.flag("no-color"));
    let format = args.value("format").unwrap_or("text");
    if !["text", "json", "junit"].contains(&format) {
        return Err(format!("--format {format}: expected text, json or junit"));
    }
    let mut patterns = implicit.to_vec();
    patterns.extend(args.positional.iter().cloned());
    let mut cases = registry::select(&patterns, &args.values("exclude"));
    let strict_left_out = if args.flag("strict") {
        0
    } else {
        let before = cases.len();
        cases.retain(|c| !c.strict);
        before - cases.len()
    };
    if cases.is_empty() {
        return Err(format!("no case matches {patterns:?}"));
    }
    let Some(target) = target_from(&args)? else {
        return Err("no broker: use --broker HOST:PORT or --spawn 'CMD {port}' \
                    (see 'mqtt-testkit help run')"
            .into());
    };
    mqtt_testkit::set_waits(
        args.number("timeout-ms")?.map(Duration::from_millis),
        args.number("quiet-ms")?.map(Duration::from_millis),
    );
    let options = runner::Options {
        jobs: args
            .number("jobs")?
            .map_or_else(runner::default_jobs, |n| usize::try_from(n).unwrap_or(1)),
        case_timeout: Duration::from_secs(args.number("case-timeout")?.unwrap_or(120)),
        fail_fast: args.flag("fail-fast"),
    };
    let verbosity = if args.flag("quiet") {
        Verbosity::Quiet
    } else if args.flag("verbose") {
        Verbosity::Verbose
    } else {
        Verbosity::Normal
    };
    let text = format == "text";
    preflight(&target)?;
    if text {
        println!(
            "{} {} against {} ({} jobs)\n",
            term::bold(TOOL),
            term::dim(&format!(
                "· {} cases{}",
                cases.len(),
                if strict_left_out > 0 {
                    format!(" ({strict_left_out} strict-tier cases left out; --strict runs them)")
                } else {
                    String::new()
                }
            )),
            term::cyan(&describe(&target)),
            options.jobs
        );
    }
    let started_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let start = Instant::now();
    let results = execute(
        &cases,
        &options,
        if text { verbosity } else { Verbosity::Quiet },
        text,
    );
    let meta = Meta {
        tool: TOOL.to_owned(),
        target: describe(&target),
        started_unix,
        duration: start.elapsed(),
        selection: patterns,
    };
    if let Some(path) = args.value("json") {
        std::fs::write(path, report::json(&meta, &results))
            .map_err(|e| format!("cannot write {path}: {e}"))?;
    }
    if let Some(path) = args.value("junit") {
        std::fs::write(path, report::junit(&meta, &results))
            .map_err(|e| format!("cannot write {path}: {e}"))?;
    }
    match format {
        "json" => print!("{}", report::json(&meta, &results)),
        "junit" => print!("{}", report::junit(&meta, &results)),
        _ => print_summary(&meta, &results, &cases),
    }
    Ok(if Summary::of(&results).ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

fn status_mark(status: Status) -> String {
    match status {
        Status::Passed => term::green("PASS"),
        Status::Failed => term::red("FAIL"),
        Status::Skipped => term::yellow("SKIP"),
        Status::TimedOut => term::red("TIME"),
    }
}

/// Runs `cases`, printing progress per `verbosity`.
pub(crate) fn execute(
    cases: &[&'static Case],
    options: &runner::Options,
    verbosity: Verbosity,
    progress_line: bool,
) -> Vec<CaseResult> {
    let total = cases.len();
    let mut done = 0;
    let mut counts = Summary::default();
    let mut running: Vec<&'static str> = Vec::new();
    let live = progress_line && term::is_tty() && verbosity != Verbosity::Verbose;
    let results = runner::run(cases, options, |event| match event {
        Event::Started(c) => {
            running.push(c.id);
        }
        Event::Finished(r) => {
            running.retain(|id| *id != r.case.id);
            done += 1;
            counts.add(r);
            let show = match verbosity {
                Verbosity::Verbose => true,
                Verbosity::Normal => r.status.is_failure(),
                Verbosity::Quiet => false,
            };
            if show {
                print!("{}", term::clear_line());
                let mut row = format!(
                    "{} {} {}",
                    status_mark(r.status),
                    r.case.id,
                    term::dim(&format!("({} ms)", r.duration.as_millis()))
                );
                if r.status == Status::Skipped
                    && let Some(m) = &r.message
                {
                    let _ = write!(row, " {}", term::dim(&format!("— {m}")));
                }
                println!("{row}");
                if r.status.is_failure()
                    && let Some(m) = &r.message
                {
                    print!("{}", term::dim(&indent(m, "       ")));
                }
                if verbosity == Verbosity::Verbose {
                    for n in &r.notes {
                        println!("       {}", term::dim(&format!("note: {n}")));
                    }
                }
            }
            if live {
                let current = running.first().copied().unwrap_or("");
                print!(
                    "{}[{done:>4}/{total}] {} {} {} {}",
                    term::clear_line(),
                    term::green(&format!("✓{}", counts.passed)),
                    term::red(&format!("✗{}", counts.failed + counts.timed_out)),
                    term::yellow(&format!("○{}", counts.skipped)),
                    term::dim(&term::truncate(current, term::width().saturating_sub(40)))
                );
                let _ = std::io::Write::flush(&mut std::io::stdout());
            }
        }
    });
    if live {
        print!("{}", term::clear_line());
    }
    results
}

fn indent(s: &str, prefix: &str) -> String {
    s.lines().fold(String::new(), |mut out, l| {
        let _ = writeln!(out, "{prefix}{l}");
        out
    })
}

#[allow(clippy::too_many_lines, reason = "linear report layout")]
fn print_summary(meta: &Meta, results: &[CaseResult], selected: &[&'static Case]) {
    let cat = spec::catalogue();
    println!("\n{}", term::bold("Results"));
    let mut groups: BTreeMap<String, Summary> = BTreeMap::new();
    for r in results {
        let key = if registry::is_interop(r.case) {
            format!("interop ({})", r.case.area)
        } else {
            format!("MQTT {}", r.case.spec())
        };
        groups.entry(key).or_default().add(r);
    }
    for (name, s) in &groups {
        println!(
            "  {:<22} {:>4} cases  {}  {}  {}",
            name,
            s.total,
            term::green(&format!("{:>4} passed", s.passed)),
            if s.failed + s.timed_out > 0 {
                term::red(&format!("{:>3} failed", s.failed + s.timed_out))
            } else {
                term::dim(&format!("{:>3} failed", 0))
            },
            term::yellow(&format!("{:>3} skipped", s.skipped)),
        );
    }
    let failures: Vec<&CaseResult> = results.iter().filter(|r| r.status.is_failure()).collect();
    if !failures.is_empty() {
        println!("\n{}", term::bold(&term::red("Failures")));
        for r in &failures {
            println!("  {} {}", status_mark(r.status), term::bold(r.case.id));
            if let Some(m) = &r.message {
                print!("{}", indent(m, "      "));
            }
            for key in r.case.reqs {
                if let Some(q) = cat.get(key) {
                    println!(
                        "      {} {} {}",
                        term::cyan(key),
                        term::dim(q.level),
                        term::dim(&term::truncate(q.summary, term::width().saturating_sub(40)))
                    );
                }
            }
            println!(
                "      {}",
                term::dim(&format!(
                    "{}:{} · mqtt-testkit show {}",
                    r.case.file, r.case.line, r.case.id
                ))
            );
        }
    }
    let verdicts = report::verdicts(results);
    let violated = verdicts
        .values()
        .filter(|v| **v == Verdict::Violated)
        .count();
    let met = verdicts.values().filter(|v| **v == Verdict::Met).count();
    let inconclusive = verdicts
        .values()
        .filter(|v| **v == Verdict::Inconclusive)
        .count();
    let s = Summary::of(results);
    println!(
        "\n{} {} checked: {} met, {} violated, {} inconclusive",
        term::bold("Requirements"),
        verdicts.len(),
        term::green(&met.to_string()),
        if violated > 0 {
            term::red(&violated.to_string())
        } else {
            violated.to_string()
        },
        term::yellow(&inconclusive.to_string()),
    );
    if inconclusive > 0 {
        println!(
            "  {}",
            term::dim(
                "inconclusive = every case checking it was skipped (feature not offered or not configurable)"
            )
        );
    }
    let all_selected = selected.len() == registry::cases().len()
        && selected.iter().all(|c| !registry::is_interop(c));
    println!(
        "\n{} in {:.1} s",
        if s.ok() {
            term::bold(&term::green(&format!(
                "✓ {} passed, {} skipped, no failures",
                s.passed, s.skipped
            )))
        } else {
            term::bold(&term::red(&format!(
                "✗ {} failed ({} requirement{} violated), {} passed, {} skipped",
                s.failed + s.timed_out,
                violated,
                if violated == 1 { "" } else { "s" },
                s.passed,
                s.skipped
            )))
        },
        meta.duration.as_secs_f64()
    );
    if s.strict_not_rejected > 0 {
        println!(
            "{}",
            term::dim(&format!(
                "  {} strict-tier case{} failed: client violations the broker did not reject \
                 (allowed; not counted as violations)",
                s.strict_not_rejected,
                if s.strict_not_rejected == 1 { "" } else { "s" }
            ))
        );
    }
    if !all_selected && s.ok() {
        println!(
            "{}",
            term::dim(
                "  (a subset of the suite ran; run without patterns for the full conformance check)"
            )
        );
    }
}

fn cmd_interop(raw: &[String]) -> Result<ExitCode, String> {
    let (suites, mut rest): (Vec<String>, Vec<String>) = raw
        .iter()
        .cloned()
        .partition(|a| a == "scenarios" || a == "mosquitto");
    let implicit: Vec<String> = if suites.is_empty() {
        vec!["interop".into()]
    } else {
        suites
            .iter()
            .flat_map(|s| match s.as_str() {
                "scenarios" => vec!["3.1.1/interop".to_owned(), "5.0/interop".to_owned()],
                other => vec![format!("interop/{other}")],
            })
            .collect()
    };
    if !rest.iter().any(|a| a == "-v" || a == "--verbose") {
        rest.push("--verbose".into());
    }
    cmd_run(&rest, &implicit)
}

// --------------------------------------------------------------- list ---

fn cmd_list(raw: &[String]) -> Result<ExitCode, String> {
    let args = Args::parse(
        raw,
        &[
            flag("long", Some('l')),
            value("exclude", Some('x')),
            flag("no-color", None),
        ],
    )?;
    term::init(args.flag("no-color"));
    let cases = registry::select(&args.positional, &args.values("exclude"));
    if cases.is_empty() {
        return Err(format!("no case matches {:?}", args.positional));
    }
    let mut last_group = String::new();
    for c in &cases {
        let group = c.id.rsplit_once('/').map_or("", |(g, _)| g);
        if group != last_group {
            let title = if registry::is_interop(c) {
                group.to_owned()
            } else {
                format!("{group} · {}", area_title(c.area))
            };
            println!("\n{}", term::bold(&title));
            group.clone_into(&mut last_group);
        }
        println!(
            "  {}  {}{}",
            c.id,
            term::dim(&format!("[{}]", c.reqs.len())),
            if c.strict {
                term::yellow(" strict")
            } else {
                String::new()
            }
        );
        if args.flag("long") {
            for r in c.reqs {
                let summary = spec::catalogue().get(r).map_or("", |q| q.summary);
                println!(
                    "      {} {}",
                    term::cyan(r),
                    term::dim(&term::truncate(summary, term::width().saturating_sub(30)))
                );
            }
        }
    }
    println!(
        "\n{} cases {}",
        cases.len(),
        term::dim("([n] = requirements checked; 'show <id>' for details)")
    );
    Ok(ExitCode::SUCCESS)
}

// --------------------------------------------------------------- show ---

fn cmd_show(raw: &[String]) -> Result<ExitCode, String> {
    let args = Args::parse(raw, &[flag("no-color", None)])?;
    term::init(args.flag("no-color"));
    let Some(query) = args.positional.first() else {
        return Err("show needs a case id or requirement id".into());
    };
    print!("{}", show(query));
    Ok(ExitCode::SUCCESS)
}

/// Renders a case, a requirement or search results for `query`.
pub(crate) fn show(query: &str) -> String {
    let query = query.trim();
    if let Some(c) = registry::find(query) {
        return render_case(c);
    }
    let cat = spec::catalogue();
    if let Some(r) = cat.get(query) {
        return render_requirement(r);
    }
    if registry::is_requirement_pattern(query) && !query.contains('/') {
        let hits: Vec<_> = cat.requirements.iter().filter(|r| r.id == query).collect();
        if !hits.is_empty() {
            return hits
                .iter()
                .map(|r| render_requirement(r))
                .collect::<Vec<_>>()
                .join("\n");
        }
    }
    search(query)
}

/// Case ids and requirements containing `query` (case-insensitive).
pub(crate) fn search(query: &str) -> String {
    let q = query.to_lowercase();
    let cases: Vec<&Case> = registry::all()
        .filter(|c| c.id.to_lowercase().contains(&q) || c.doc.to_lowercase().contains(&q))
        .collect();
    if cases.len() == 1
        && let Some(c) = cases.first()
    {
        return render_case(c);
    }
    let reqs: Vec<&spec::Requirement> = spec::catalogue()
        .requirements
        .iter()
        .filter(|r| r.key.to_lowercase().contains(&q) || r.summary.to_lowercase().contains(&q))
        .collect();
    let mut out = String::new();
    if cases.is_empty() && reqs.is_empty() {
        let _ = writeln!(out, "nothing matches {query:?}");
        return out;
    }
    if !cases.is_empty() {
        let _ = writeln!(out, "{}", term::bold(&format!("Cases ({})", cases.len())));
        for c in cases.iter().take(40) {
            let _ = writeln!(out, "  {}", c.id);
        }
        if cases.len() > 40 {
            let _ = writeln!(
                out,
                "  {}",
                term::dim(&format!("… {} more", cases.len() - 40))
            );
        }
    }
    if !reqs.is_empty() {
        let _ = writeln!(
            out,
            "{}",
            term::bold(&format!("Requirements ({})", reqs.len()))
        );
        for r in reqs.iter().take(40) {
            let _ = writeln!(
                out,
                "  {:<22} {}",
                term::cyan(&r.key),
                term::truncate(r.summary, term::width().saturating_sub(26))
            );
        }
        if reqs.len() > 40 {
            let _ = writeln!(
                out,
                "  {}",
                term::dim(&format!("… {} more", reqs.len() - 40))
            );
        }
    }
    out
}

/// A case, its description and its requirements.
pub(crate) fn render_case(c: &Case) -> String {
    let w = term::width();
    let mut out = String::new();
    let _ = writeln!(out, "{}", term::bold(c.id));
    let _ = writeln!(out, "  {}", c.title());
    let _ = writeln!(
        out,
        "  {}",
        term::dim(&format!(
            "MQTT {} · {} · {}:{}{}",
            spec_label(c.version),
            area_title(c.area),
            c.file,
            c.line,
            if c.strict {
                " · strict tier (runs with --strict)"
            } else {
                ""
            }
        ))
    );
    if !c.doc.is_empty() {
        out.push('\n');
        out.push_str(&term::wrap(c.doc, w, "  "));
    }
    if c.reqs.is_empty() {
        let _ = writeln!(out, "\n  {}", term::dim("No requirement markers."));
    } else {
        let _ = writeln!(
            out,
            "\n  {}",
            term::bold(&format!("Checks {} requirement(s)", c.reqs.len()))
        );
        let cat = spec::catalogue();
        for key in c.reqs {
            match cat.get(key) {
                Some(r) => {
                    let _ = writeln!(out, "  {} {}", term::cyan(key), term::dim(r.level));
                    out.push_str(&term::wrap(r.summary, w, "      "));
                    let _ = writeln!(out, "      {}", term::dim(r.url));
                }
                None => {
                    let _ = writeln!(out, "  {key} (not in the catalogue)");
                }
            }
        }
    }
    let _ = writeln!(
        out,
        "\n  {}",
        term::dim(&format!(
            "Run it: mqtt-testkit run {} --broker HOST:PORT",
            c.id
        ))
    );
    out
}

/// A requirement and the cases that check it.
pub(crate) fn render_requirement(r: &spec::Requirement) -> String {
    let w = term::width();
    let mut out = String::new();
    let _ = writeln!(out, "{} {}", term::bold(&r.key), term::dim(r.level));
    let _ = writeln!(
        out,
        "  {}",
        term::dim(&format!(
            "MQTT {} §{} · {} · binds {}{}",
            spec_label(r.version),
            r.section,
            r.area,
            r.actor,
            if r.server_binding {
                ""
            } else {
                " (not server-binding)"
            }
        ))
    );
    out.push_str(&term::wrap(r.summary, w, "  "));
    let _ = writeln!(out, "  {}", term::dim(r.url));
    let cases = spec::cases_for(&r.key);
    if cases.is_empty() {
        let text = match (r.server_binding, r.exemption) {
            (true, Some((category, reason))) => {
                term::dim(&format!("No case ({}): {reason}", category.label()))
            }
            (true, None) => term::yellow("No case checks this requirement yet."),
            (false, _) => term::dim("No case: it binds clients, not servers."),
        };
        let _ = writeln!(out, "\n  {text}");
    } else {
        let _ = writeln!(
            out,
            "\n  {}",
            term::bold(&format!("Checked by {} case(s)", cases.len()))
        );
        for c in cases {
            let _ = writeln!(out, "  {}", c.id);
        }
    }
    out
}

// --------------------------------------------------------------- reqs ---

fn cmd_reqs(raw: &[String]) -> Result<ExitCode, String> {
    let args = Args::parse(
        raw,
        &[
            value("version", None),
            value("area", None),
            flag("uncovered", None),
            flag("all", None),
            flag("no-color", None),
        ],
    )?;
    term::init(args.flag("no-color"));
    let version = args
        .value("version")
        .map(|v| {
            registry::parse_spec(v)
                .ok_or_else(|| format!("--version {v}: expected 3.1, 3.1.1 or 5.0"))
        })
        .transpose()?;
    let cat = spec::catalogue();
    let mut shown = 0;
    let mut last_version = None;
    for r in &cat.requirements {
        if version.is_some_and(|v| v != r.version)
            || args.value("area").is_some_and(|a| a != r.area)
            || (!args.flag("all") && !r.server_binding)
        {
            continue;
        }
        if !args.positional.is_empty()
            && !args
                .positional
                .iter()
                .any(|p| registry::glob(p, &r.key) || registry::glob(p, r.id))
        {
            continue;
        }
        let n = spec::cases_for(&r.key).len();
        if args.flag("uncovered") && n > 0 {
            continue;
        }
        if last_version != Some(r.version) {
            let title = cat.source(r.version).map_or_else(
                || format!("MQTT {}", spec_label(r.version)),
                |s| s.title.to_owned(),
            );
            println!("\n{}", term::bold(&title));
            last_version = Some(r.version);
        }
        let count = if n == 0 {
            term::yellow("  –")
        } else {
            term::green(&format!("{n:>3}"))
        };
        println!(
            "  {:<24} {:<10} {}  {}",
            term::cyan(&r.key),
            r.level,
            count,
            term::truncate(r.summary, term::width().saturating_sub(46))
        );
        shown += 1;
    }
    println!(
        "\n{shown} requirement(s) {}",
        term::dim("(column 3 = cases checking it; 'show <id>' for details)")
    );
    Ok(ExitCode::SUCCESS)
}

fn cmd_coverage(raw: &[String]) -> Result<ExitCode, String> {
    let args = Args::parse(raw, &[flag("verbose", Some('v')), flag("no-color", None)])?;
    term::init(args.flag("no-color"));
    print!("{}", coverage_table(args.flag("verbose")));
    Ok(ExitCode::SUCCESS)
}

/// The coverage table (and, when `list`, the unchecked requirements).
pub(crate) fn coverage_table(list: bool) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{}",
        term::bold(&format!(
            "{:<10} {:>6} {:>6} {:>8} {:>8} {:>7} {:>5}  {}",
            "version", "cases", "reqs", "server", "checked", "exempt", "open", "checked"
        ))
    );
    for c in spec::coverage() {
        let cases = registry::cases()
            .iter()
            .filter(|x| x.version == c.version)
            .count();
        #[allow(clippy::cast_precision_loss, reason = "requirement counts are tiny")]
        let pct = if c.server_binding == 0 {
            100.0
        } else {
            c.covered as f64 * 100.0 / c.server_binding as f64
        };
        let bar_len = 20;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "0..=20"
        )]
        let filled = (pct / 5.0).round() as usize;
        let bar = format!(
            "{}{}",
            "█".repeat(filled.min(bar_len)),
            "░".repeat(bar_len.saturating_sub(filled))
        );
        let _ = writeln!(
            out,
            "{:<10} {:>6} {:>6} {:>8} {:>8} {:>7} {:>5}  {} {:5.1}%",
            format!("MQTT {}", spec_label(c.version)),
            cases,
            c.total,
            c.server_binding,
            c.covered,
            c.exempt,
            c.server_binding - c.covered - c.exempt,
            if c.covered + c.exempt == c.server_binding {
                term::green(&bar)
            } else {
                term::yellow(&bar)
            },
            pct
        );
    }
    let _ = writeln!(
        out,
        "\n{}",
        term::dim(
            "server = requirements a broker can violate · checked = cited by at least one case\n\
             exempt = documented as not checkable black-box (spec/applicability.tsv) · open = neither"
        )
    );
    if list {
        let cat = spec::catalogue();
        let missing: Vec<&spec::Requirement> = cat
            .requirements
            .iter()
            .filter(|r| r.server_binding && spec::cases_for(&r.key).is_empty())
            .collect();
        let (exempt, open): (Vec<_>, Vec<_>) =
            missing.into_iter().partition(|r| r.exemption.is_some());
        let _ = writeln!(
            out,
            "\n{}",
            term::bold(&format!("Open: no case, no exemption ({})", open.len()))
        );
        for r in open {
            let _ = writeln!(
                out,
                "  {:<24} {:<10} {}",
                term::cyan(&r.key),
                r.level,
                term::truncate(r.summary, term::width().saturating_sub(40))
            );
        }
        let _ = writeln!(
            out,
            "\n{}",
            term::bold(&format!("Exempt ({})", exempt.len()))
        );
        for r in exempt {
            if let Some((category, reason)) = r.exemption {
                let _ = writeln!(
                    out,
                    "  {:<24} {:<17} {}",
                    term::cyan(&r.key),
                    category.label(),
                    term::dim(&term::truncate(reason, term::width().saturating_sub(45)))
                );
            }
        }
    }
    out
}

// -------------------------------------------------------------- probe ---

fn cmd_probe(raw: &[String]) -> Result<ExitCode, String> {
    let mut opts = BROKER_OPTS.to_vec();
    opts.push(flag("no-color", None));
    let args = Args::parse(raw, &opts)?;
    term::init(args.flag("no-color"));
    let Some(target) = target_from(&args)? else {
        return Err("no broker: use --broker HOST:PORT or --spawn 'CMD {port}'".into());
    };
    preflight(&target)?;
    print!("{}", probe(&target));
    Ok(ExitCode::SUCCESS)
}

fn prop_name(id: u8) -> &'static str {
    match id {
        prop::SESSION_EXPIRY_INTERVAL => "Session Expiry Interval",
        prop::ASSIGNED_CLIENT_IDENTIFIER => "Assigned Client Identifier",
        prop::SERVER_KEEP_ALIVE => "Server Keep Alive",
        prop::AUTHENTICATION_METHOD => "Authentication Method",
        prop::AUTHENTICATION_DATA => "Authentication Data",
        prop::RESPONSE_INFORMATION => "Response Information",
        prop::SERVER_REFERENCE => "Server Reference",
        prop::REASON_STRING => "Reason String",
        prop::RECEIVE_MAXIMUM => "Receive Maximum",
        prop::TOPIC_ALIAS_MAXIMUM => "Topic Alias Maximum",
        prop::MAXIMUM_QOS => "Maximum QoS",
        prop::RETAIN_AVAILABLE => "Retain Available",
        prop::USER_PROPERTY => "User Property",
        prop::MAXIMUM_PACKET_SIZE => "Maximum Packet Size",
        prop::WILDCARD_SUBSCRIPTION_AVAILABLE => "Wildcard Subscription Available",
        prop::SUBSCRIPTION_IDENTIFIER_AVAILABLE => "Subscription Identifier Available",
        prop::SHARED_SUBSCRIPTION_AVAILABLE => "Shared Subscription Available",
        _ => "property",
    }
}

fn prop_value(v: &PropValue) -> String {
    match v {
        PropValue::Byte(b) => b.to_string(),
        PropValue::TwoByte(n) => n.to_string(),
        PropValue::FourByte(n) | PropValue::VarInt(n) => n.to_string(),
        PropValue::Str(s) => format!("{s:?}"),
        PropValue::Bin(b) => format!("{} bytes", b.len()),
        PropValue::Pair(k, v) => format!("{k:?} = {v:?}"),
    }
}

/// Connects once per version and describes each CONNACK.
pub(crate) fn probe(target: &Target) -> String {
    let mut out = format!("{} {}\n", term::bold("Broker"), describe(target));
    for version in registry::VERSIONS {
        let label = format!("MQTT {}", spec_label(version));
        let result = Broker::from_env()
            .map_err(|e| e.to_string())
            .and_then(|b| b.ok_or_else(|| "no broker".to_owned()))
            .and_then(|b| {
                b.connect(&Connect::new(version, &unique_id("probe")))
                    .map(|(c, ack)| {
                        let _ = c.disconnect();
                        ack
                    })
                    .map_err(|e| e.to_string())
            });
        match result {
            Ok(ack) => {
                let ok = ack.code == 0;
                let _ = writeln!(
                    out,
                    "\n{} {} {}",
                    term::bold(&label),
                    if ok {
                        term::green("accepted")
                    } else {
                        term::red("refused")
                    },
                    term::dim(&format!(
                        "(CONNACK code 0x{:02X}, session present {})",
                        ack.code,
                        ack.session_present()
                    ))
                );
                if version == Version::V5 {
                    if ack.properties.is_empty() {
                        let _ = writeln!(
                            out,
                            "  {}",
                            term::dim("no CONNACK properties (all defaults)")
                        );
                    }
                    for p in &ack.properties {
                        let _ = writeln!(out, "  {:<36} {}", prop_name(p.id), prop_value(&p.value));
                    }
                    let announced = |id| ack.properties.iter().any(|p| p.id == id);
                    let defaults: Vec<&str> = [
                        (prop::RECEIVE_MAXIMUM, "Receive Maximum 65535"),
                        (prop::MAXIMUM_QOS, "Maximum QoS 2"),
                        (prop::RETAIN_AVAILABLE, "Retain Available 1"),
                        (prop::MAXIMUM_PACKET_SIZE, "no Maximum Packet Size"),
                        (prop::TOPIC_ALIAS_MAXIMUM, "Topic Alias Maximum 0"),
                        (prop::WILDCARD_SUBSCRIPTION_AVAILABLE, "wildcards 1"),
                        (
                            prop::SUBSCRIPTION_IDENTIFIER_AVAILABLE,
                            "subscription ids 1",
                        ),
                        (
                            prop::SHARED_SUBSCRIPTION_AVAILABLE,
                            "shared subscriptions 1",
                        ),
                    ]
                    .into_iter()
                    .filter(|(id, _)| !announced(*id))
                    .map(|(_, d)| d)
                    .collect();
                    if !defaults.is_empty() {
                        let _ = writeln!(
                            out,
                            "  {}",
                            term::dim(&format!(
                                "defaults (not announced): {}",
                                defaults.join(", ")
                            ))
                        );
                    }
                }
            }
            Err(e) => {
                let _ = writeln!(out, "\n{} {} {}", term::bold(&label), term::red("error"), e);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Args {
        let raw: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();
        Args::parse(&raw, &RUN_OPTS).unwrap()
    }

    #[test]
    fn spawn_target_splits_program_and_arguments() {
        let t = target_from(&parse(&["--spawn", "mosquitto -p {port}"])).unwrap();
        assert_eq!(
            t,
            Some(Target::Spawn {
                program: "mosquitto".into(),
                args: "-p {port}".into()
            })
        );
        assert!(target_from(&parse(&["-b", "h:1", "-s", "x"])).is_err());
        assert_eq!(
            target_from(&parse(&["-b", "h:1"])).unwrap(),
            Some(Target::Address("h:1".into()))
        );
    }

    #[test]
    fn show_finds_cases_requirements_and_searches() {
        let case = registry::cases()[0];
        assert!(show(case.id).contains(case.id));
        assert!(show("5.0/MQTT-3.1.2-1").contains("5.0/MQTT-3.1.2-1"));
        let both = show("MQTT-3.1.2-1");
        assert!(both.contains("3.1.1/MQTT-3.1.2-1") && both.contains("5.0/MQTT-3.1.2-1"));
        assert!(show("retained").contains("Cases"));
        assert!(show("zzzz-no-such-thing").contains("nothing matches"));
    }

    #[test]
    fn coverage_table_lists_every_version() {
        let t = coverage_table(true);
        for v in ["MQTT 3.1 ", "MQTT 3.1.1", "MQTT 5.0"] {
            assert!(t.contains(v), "{t}");
        }
        assert!(t.contains("Open:") && t.contains("Exempt"));
    }

    #[test]
    fn every_command_has_help() {
        for c in [
            "run", "interop", "list", "show", "reqs", "coverage", "probe", "browse",
        ] {
            assert!(USAGE.contains(c), "{c}");
        }
    }
}
