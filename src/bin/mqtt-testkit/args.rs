// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! A small command-line parser (the crate has no dependencies).

use std::collections::BTreeMap;

/// Parsed arguments of one command.
#[derive(Debug, Default)]
pub struct Args {
    /// Positional arguments, in order.
    pub positional: Vec<String>,
    /// Options with values, by long name; repeated options accumulate.
    values: BTreeMap<&'static str, Vec<String>>,
    /// Flags that were given, by long name.
    flags: Vec<&'static str>,
}

/// One accepted option.
#[derive(Debug, Clone, Copy)]
pub struct Opt {
    /// Long name without dashes.
    pub long: &'static str,
    /// Optional one-letter short name.
    pub short: Option<char>,
    /// Whether it takes a value.
    pub takes_value: bool,
}

/// An option taking a value.
pub const fn value(long: &'static str, short: Option<char>) -> Opt {
    Opt {
        long,
        short,
        takes_value: true,
    }
}

/// A flag.
pub const fn flag(long: &'static str, short: Option<char>) -> Opt {
    Opt {
        long,
        short,
        takes_value: false,
    }
}

impl Args {
    /// Parses `raw` (without the program and command names) against `opts`.
    /// `--` ends option parsing.
    pub fn parse(raw: &[String], opts: &[Opt]) -> Result<Self, String> {
        let mut out = Self::default();
        let mut i = 0;
        let mut only_positional = false;
        while let Some(arg) = raw.get(i) {
            i += 1;
            if only_positional || arg == "-" || !arg.starts_with('-') {
                out.positional.push(arg.clone());
                continue;
            }
            if arg == "--" {
                only_positional = true;
                continue;
            }
            let (opt, inline) = if let Some(long) = arg.strip_prefix("--") {
                let (name, inline) = match long.split_once('=') {
                    Some((n, v)) => (n, Some(v.to_owned())),
                    None => (long, None),
                };
                let opt = opts
                    .iter()
                    .find(|o| o.long == name)
                    .ok_or_else(|| format!("unknown option --{name}"))?;
                (opt, inline)
            } else {
                let mut chars = arg.chars().skip(1);
                let c = chars.next().ok_or("empty option")?;
                let rest: String = chars.collect();
                let opt = opts
                    .iter()
                    .find(|o| o.short == Some(c))
                    .ok_or_else(|| format!("unknown option -{c}"))?;
                (opt, (!rest.is_empty()).then_some(rest))
            };
            if opt.takes_value {
                let v = if let Some(v) = inline {
                    v
                } else {
                    let v = raw
                        .get(i)
                        .ok_or_else(|| format!("--{} needs a value", opt.long))?;
                    i += 1;
                    v.clone()
                };
                out.values.entry(opt.long).or_default().push(v);
            } else {
                if inline.is_some() {
                    return Err(format!("--{} takes no value", opt.long));
                }
                out.flags.push(opt.long);
            }
        }
        Ok(out)
    }

    /// The last value of `long`.
    pub fn value(&self, long: &str) -> Option<&str> {
        self.values
            .get(long)
            .and_then(|v| v.last())
            .map(String::as_str)
    }

    /// Every value of `long`, in order; comma-separated values are split.
    pub fn values(&self, long: &str) -> Vec<String> {
        self.values
            .get(long)
            .map(|v| {
                v.iter()
                    .flat_map(|s| s.split(','))
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether flag `long` was given.
    pub fn flag(&self, long: &str) -> bool {
        self.flags.contains(&long)
    }

    /// The value of `long` parsed as a number.
    pub fn number(&self, long: &str) -> Result<Option<u64>, String> {
        self.value(long)
            .map(|v| {
                v.trim()
                    .parse()
                    .map_err(|_| format!("--{long}: {v:?} is not a number"))
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPTS: &[Opt] = &[
        value("broker", Some('b')),
        value("exclude", Some('x')),
        flag("fail-fast", None),
        flag("verbose", Some('v')),
    ];

    fn parse(args: &[&str]) -> Result<Args, String> {
        let raw: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();
        Args::parse(&raw, OPTS)
    }

    #[test]
    fn long_short_inline_and_positional() {
        let a = parse(&[
            "5.0",
            "-b",
            "h:1",
            "--exclude=a,b",
            "-xc",
            "-v",
            "--fail-fast",
            "--",
            "-y",
        ])
        .unwrap();
        assert_eq!(a.positional, ["5.0", "-y"]);
        assert_eq!(a.value("broker"), Some("h:1"));
        assert_eq!(a.values("exclude"), ["a", "b", "c"]);
        assert!(a.flag("verbose") && a.flag("fail-fast"));
        assert!(!a.flag("broker"));
    }

    #[test]
    fn errors() {
        assert!(parse(&["--nope"]).unwrap_err().contains("unknown option"));
        assert!(parse(&["-b"]).unwrap_err().contains("needs a value"));
        assert!(
            parse(&["--verbose=1"])
                .unwrap_err()
                .contains("takes no value")
        );
        let a = parse(&["-b", "x"]).unwrap();
        assert!(a.number("broker").is_err());
    }
}
