#!/usr/bin/env python3
"""Fail CI when Criterion reports statistically meaningful regressions."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Criterion change estimates against a max regression threshold."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("target/criterion"),
        help="Criterion output root directory.",
    )
    parser.add_argument(
        "--max-regression",
        type=float,
        default=0.15,
        help="Maximum allowed regression ratio (0.15 == 15%%).",
    )
    return parser.parse_args()


def benchmark_name(change_estimates_path: Path, root: Path) -> str:
    # target/criterion/<bench>/change/estimates.json -> <bench>
    relative = change_estimates_path.relative_to(root)
    return "/".join(relative.parts[:-2])


def format_pct(value: float) -> str:
    return f"{value * 100:+.2f}%"


def main() -> int:
    args = parse_args()
    root = args.root
    max_regression = args.max_regression

    change_paths = sorted(root.glob("**/change/estimates.json"))
    if not change_paths:
        print(
            f"ERROR: no Criterion change estimates found under {root}. "
            "Run benchmarks with --baseline <name> first.",
            file=sys.stderr,
        )
        return 2

    rows = []
    regressions = []

    for change_path in change_paths:
        with change_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        mean = data["mean"]
        point = float(mean["point_estimate"])
        lower = float(mean["confidence_interval"]["lower_bound"])
        upper = float(mean["confidence_interval"]["upper_bound"])

        # Gate only when the full confidence interval exceeds threshold.
        is_regression = lower > max_regression
        name = benchmark_name(change_path, root)

        rows.append((name, point, lower, upper, is_regression))
        if is_regression:
            regressions.append((name, point, lower, upper))

    lines = []
    lines.append("## Criterion Regression Check")
    lines.append("")
    lines.append(f"- Threshold: `{max_regression * 100:.1f}%` (mean CI lower bound)")
    lines.append(f"- Benchmarks analyzed: `{len(rows)}`")
    lines.append("")
    lines.append("| Benchmark | Mean Change | CI Low | CI High | Status |")
    lines.append("|---|---:|---:|---:|---|")
    for name, point, lower, upper, is_regression in rows:
        status = "REGRESSION" if is_regression else "ok"
        lines.append(
            f"| `{name}` | `{format_pct(point)}` | `{format_pct(lower)}` | `{format_pct(upper)}` | `{status}` |"
        )

    lines.append("")
    if regressions:
        lines.append(
            f"Result: FAIL ({len(regressions)} benchmark(s) exceeded the regression threshold)."
        )
    else:
        lines.append("Result: PASS (no benchmark exceeded the regression threshold).")

    summary = "\n".join(lines)
    print(summary)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary_file:
            summary_file.write(summary + "\n")

    return 1 if regressions else 0


if __name__ == "__main__":
    raise SystemExit(main())
