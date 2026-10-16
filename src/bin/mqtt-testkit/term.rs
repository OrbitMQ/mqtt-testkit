// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 SOFTT

//! Terminal styling: ANSI colours when stdout is a terminal (and `NO_COLOR`
//! is unset), plain text otherwise.

use std::io::IsTerminal as _;
use std::sync::atomic::{AtomicBool, Ordering};

static COLOR: AtomicBool = AtomicBool::new(false);
static TTY: AtomicBool = AtomicBool::new(false);

/// Decides once whether to colour output.
pub fn init(no_color: bool) {
    let tty = std::io::stdout().is_terminal();
    TTY.store(tty, Ordering::Relaxed);
    let env_off = std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty());
    COLOR.store(tty && !no_color && !env_off, Ordering::Relaxed);
}

/// Whether stdout is a terminal.
pub fn is_tty() -> bool {
    TTY.load(Ordering::Relaxed)
}

fn paint(code: &str, s: &str) -> String {
    if COLOR.load(Ordering::Relaxed) {
        format!("\x1b[{code}m{s}\x1b[0m")
    } else {
        s.to_owned()
    }
}

pub fn bold(s: &str) -> String {
    paint("1", s)
}
pub fn dim(s: &str) -> String {
    paint("2", s)
}
pub fn red(s: &str) -> String {
    paint("31", s)
}
pub fn green(s: &str) -> String {
    paint("32", s)
}
pub fn yellow(s: &str) -> String {
    paint("33", s)
}
pub fn cyan(s: &str) -> String {
    paint("36", s)
}

/// Clears the current line (for the live progress line).
pub fn clear_line() -> &'static str {
    if is_tty() { "\r\x1b[2K" } else { "" }
}

/// Usable output width: `$COLUMNS`, else 100.
pub fn width() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|c| c.trim().parse().ok())
        .filter(|&w: &usize| w >= 40)
        .unwrap_or(100)
}

/// `s` cut to `max` characters, with an ellipsis when cut.
pub fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_owned();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// Word-wraps `s` to `width`, prefixing every line with `indent`.
pub fn wrap(s: &str, width: usize, indent: &str) -> String {
    let mut out = String::new();
    for para in s.lines() {
        let mut line = String::new();
        for word in para.split_whitespace() {
            if !line.is_empty() && indent.len() + line.len() + 1 + word.len() > width {
                out.push_str(indent);
                out.push_str(&line);
                out.push('\n');
                line.clear();
            }
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(word);
        }
        out.push_str(indent);
        out.push_str(&line);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_and_wrap() {
        assert_eq!(truncate("abcdef", 4), "abc…");
        assert_eq!(truncate("abc", 4), "abc");
        assert_eq!(wrap("aa bb cc", 7, "  "), "  aa bb\n  cc\n");
        assert_eq!(wrap("aa bb", 80, "> "), "> aa bb\n");
    }
}
