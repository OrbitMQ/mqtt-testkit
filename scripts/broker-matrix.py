#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 SOFTT
"""Render the weekly broker results into README.md.

Input: one directory per broker under RESULTS, each with
  meta.json   {"name": ..., "image": ..., "version": ..., "url": ..., "started": true|false, "error": "..."}
  run.json    `mqtt-testkit run --strict --json` report (absent if the broker did not start)
  retry.json  optional report of re-running the cases that failed in run.json

A case counts as failed only if it failed in run.json and again in retry.json (when a retry ran), so a
one-off timing blip on a shared CI runner does not flip the table. The table is deterministic (no dates,
no timings), so README.md changes only when results change.

Usage:
  scripts/broker-matrix.py RESULTS --readme README.md   # rewrite the marked section; print a summary
  scripts/broker-matrix.py RESULTS                       # print the section only
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BEGIN = "<!-- BEGIN broker-results -->"
END = "<!-- END broker-results -->"
VERSIONS = ["3.1", "3.1.1", "5.0"]
FAILED = {"failed", "timeout"}


def load(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def merge(run: dict, retry: dict | None) -> list[dict]:
    """Cases of `run`; a failure that passed (or skipped) on retry is taken from the retry."""
    cases = run.get("cases", [])
    if not retry:
        return cases
    again = {c["id"]: c for c in retry.get("cases", [])}
    return [again.get(c["id"], c) if c["status"] in FAILED else c for c in cases]


def broker_rows(results: Path) -> list[dict]:
    rows = []
    for d in sorted(p for p in results.iterdir() if p.is_dir()):
        meta = load(d / "meta.json") or {"name": d.name}
        run = load(d / "run.json")
        row = {"meta": meta, "cases": None}
        if run is not None and meta.get("started", True):
            row["cases"] = merge(run, load(d / "retry.json"))
        rows.append(row)
    rows.sort(key=lambda r: str(r["meta"].get("name", "")).lower())
    return rows


def cell(cases: list[dict], version: str) -> str:
    mine = [c for c in cases if c["version"] == version and not c.get("strict")]
    if not mine:
        return "–"
    passed = sum(c["status"] == "passed" for c in mine)
    failed = sum(c["status"] in FAILED for c in mine)
    skipped = sum(c["status"] == "skipped" for c in mine)
    mark = "✅" if failed == 0 else "❌"
    text = f"{mark} {passed}/{len(mine)}"
    if failed:
        text += f" · {failed} failed"
    if skipped:
        text += f" · {skipped} skipped"
    return text


def violated(cases: list[dict]) -> list[str]:
    reqs = set()
    for c in cases:
        if c["status"] in FAILED and not c.get("strict"):
            reqs.update(c.get("requirements", []))
    return sorted(reqs)


def render(rows: list[dict]) -> str:
    out = [
        BEGIN,
        "",
        "| Broker | Version | MQTT 3.1 | MQTT 3.1.1 | MQTT 5.0 | Requirements violated | Strict tier |",
        "|---|---|---|---|---|---|---|",
    ]
    details = []
    for row in rows:
        meta = row["meta"]
        name = meta.get("name", "?")
        if meta.get("url"):
            name = f"[{name}]({meta['url']})"
        version = f"`{meta.get('version') or 'unknown'}`"
        cases = row["cases"]
        if cases is None:
            reason = meta.get("error") or "did not start"
            out.append(f"| {name} | {version} | – | – | – | – | {reason} |")
            continue
        strict = [c for c in cases if c.get("strict")]
        strict_failed = sum(c["status"] in FAILED for c in strict)
        strict_cell = f"{len(strict) - strict_failed}/{len(strict)} rejected" if strict else "–"
        bad = violated(cases)
        out.append(
            f"| {name} | {version} | {cell(cases, '3.1')} | {cell(cases, '3.1.1')} | {cell(cases, '5.0')} "
            f"| {len(bad)} | {strict_cell} |"
        )
        failing = sorted(
            (c["id"] for c in cases if c["status"] in FAILED and not c.get("strict")),
            key=lambda cid: (VERSIONS.index(cid.split("/")[0]) if cid.split("/")[0] in VERSIONS else 9, cid),
        )
        if failing:
            details.append(f"**{meta.get('name', '?')}** ({len(failing)})\n")
            details.extend(f"- `{cid}`" for cid in failing)
            details.append("")
    out.append("")
    if details:
        out.append("<details><summary>Failing cases</summary>\n")
        out.extend(details)
        out.append("</details>\n")
    out.append(END)
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", type=Path)
    ap.add_argument("--readme", type=Path)
    args = ap.parse_args()
    section = render(broker_rows(args.results))
    if not args.readme:
        print(section)
        return 0
    text = args.readme.read_text()
    if BEGIN not in text or END not in text:
        sys.exit(f"broker-matrix: {args.readme} has no {BEGIN} ... {END} section")
    head, rest = text.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    new = head + section + tail
    if new != text:
        args.readme.write_text(new)
        print("broker-matrix: README updated")
    else:
        print("broker-matrix: no change")
    print(section)
    return 0


if __name__ == "__main__":
    sys.exit(main())
