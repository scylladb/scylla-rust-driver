#!/usr/bin/env python3
"""Compare driver benchmark results and report regressions and improvements.

Reads the machine-readable output produced by

    cargo bench -p benchmarks --bench requests -- \
        --baseline=base --output-format=json

(one JSON object per line) and writes a Markdown report comparing the pull
request against its baseline, suitable for ``$GITHUB_STEP_SUMMARY`` (see
``--summary``).

The report mirrors ``cargo bench``'s own output: one section per scenario, split
into Callgrind and DHAT tables that list every measured metric with its pull
request value, baseline value and change (percent and factor, matching the tool).

Only three metrics are checked against thresholds and carry an ``ok`` /
``regression`` / ``improvement`` status; the remaining rows are shown for
context (status ``—``). A metric is a *regression* when instructions grow by at
least ``--instructions-threshold`` percent, allocations (heap blocks) grow by
more than ``--allocations-threshold`` blocks, or peak memory grows by at least
``--peak-threshold`` percent, and an *improvement* when it shrinks by the same
margin.

When the ``GITHUB_OUTPUT`` environment variable is set, ``has_regressions`` is
written to it so the workflow can decide whether to label the pull request.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Optional

# gungraun metric keys that are checked against thresholds.
INSTRUCTIONS = "Ir"
ALLOC_BLOCKS = "TotalBlocks"
PEAK_BYTES = "AtTGmaxBytes"

# key -> (Thresholds attribute, whether the threshold is an absolute count).
_THRESHOLDED = {
    INSTRUCTIONS: ("instructions", False),
    ALLOC_BLOCKS: ("allocations", True),
    PEAK_BYTES: ("peak", False),
}

# Human-readable labels, taken from gungraun's `Display` impls for `EventKind`
# and `DhatMetric` so the report reads like the tool's own output. Keys without
# an entry (e.g. `Dr`, `Bc`) fall back to the raw variant name, exactly as
# gungraun does.
_CALLGRIND_LABELS = {
    "Ir": "Instructions",
    "L1hits": "L1 Hits",
    "LLhits": "LL Hits",
    "RamHits": "RAM Hits",
    "TotalRW": "Total read+write",
    "EstimatedCycles": "Estimated Cycles",
    "I1MissRate": "I1 Miss Rate",
    "D1MissRate": "D1 Miss Rate",
    "LLiMissRate": "LLi Miss Rate",
    "LLdMissRate": "LLd Miss Rate",
    "LLMissRate": "LL Miss Rate",
    "L1HitRate": "L1 Hit Rate",
    "LLHitRate": "LL Hit Rate",
    "RamHitRate": "RAM Hit Rate",
}
_DHAT_LABELS = {
    "TotalUnits": "Total units",
    "TotalEvents": "Total events",
    "TotalBytes": "Total bytes",
    "TotalBlocks": "Total blocks",
    "AtTGmaxBytes": "At t-gmax bytes",
    "AtTGmaxBlocks": "At t-gmax blocks",
    "AtTEndBytes": "At t-end bytes",
    "AtTEndBlocks": "At t-end blocks",
    "ReadsBytes": "Reads bytes",
    "WritesBytes": "Writes bytes",
    "TotalLifetimes": "Total lifetimes",
    "MaximumBytes": "Maximum bytes",
    "MaximumBlocks": "Maximum blocks",
}

# The tools we render, in order, with the heading gungraun prints for each.
_TOOLS = (("Callgrind", "Callgrind"), ("Dhat", "DHAT"))


def _label(tool: str, key: str) -> str:
    table = _CALLGRIND_LABELS if tool == "Callgrind" else _DHAT_LABELS
    return table.get(key, key)


@dataclass
class Metric:
    """A measured value: the new (pull request) figure and the old (baseline)
    one, plus whether it is a floating-point metric (e.g. a hit rate)."""

    new: float
    old: Optional[float]
    is_float: bool = False

    @property
    def delta_pct(self) -> Optional[float]:
        if self.old is None or self.old == 0:
            return None
        return (self.new - self.old) / self.old * 100.0

    @property
    def delta_abs(self) -> Optional[float]:
        if self.old is None:
            return None
        return self.new - self.old


@dataclass
class MetricRow:
    key: str  # gungraun variant name, e.g. "Ir"
    label: str  # display label, e.g. "Instructions"
    tool: str  # "Callgrind" or "Dhat"
    metric: Metric


@dataclass
class Benchmark:
    function: str  # benchmark function name, e.g. "insert"
    details: str  # setup call, e.g. "setup_default(100)"
    name: str  # readable scenario name, used in logs
    rows: list[MetricRow]


def _metric(metrics: dict) -> Optional[Metric]:
    """Builds a :class:`Metric` from a gungraun metric entry.

    A comparison against a baseline yields ``{"Both": [new, old]}``; a run with
    no baseline yields ``{"Left": new}``. Each value is wrapped as ``{"Int": n}``
    or ``{"Float": x}``.
    """

    def number(value: dict) -> tuple[float, bool]:
        if "Int" in value:
            return float(value["Int"]), False
        return float(value["Float"]), True

    if "Both" in metrics:
        new, old = metrics["Both"]
        new_val, new_float = number(new)
        old_val, old_float = number(old)
        return Metric(new_val, old_val, is_float=new_float or old_float)
    if "Left" in metrics:
        # `Left` is the bare value `{"Int"/"Float": n}`. Older payloads wrapped
        # it in a single-element list, so tolerate that shape too.
        left = metrics["Left"]
        if isinstance(left, list):
            left = left[0]
        new_val, new_float = number(left)
        return Metric(new_val, None, is_float=new_float)
    return None


def _parse_args(details: str) -> list[int]:
    """Extracts the integer arguments recorded in a setup call such as
    ``setup_default(1000)`` or ``setup_batch(8, 1000)``."""
    match = re.search(r"\(([^)]*)\)", details)
    if not match:
        return []
    return [int(tok) for tok in re.findall(r"-?\d+", match.group(1))]


def _scenario_name(function: str, params: tuple[int, ...]) -> str:
    # The ``batch`` scenario is parameterised by its batch size (its leading
    # setup argument).
    if function == "batch" and params:
        return f"batch(size={params[0]})"
    return function


def _benchmark(record: dict) -> Benchmark:
    """Parses one JSON record into a :class:`Benchmark`, keeping every metric."""
    function = record["function_name"]
    details = record.get("details") or ""
    # The trailing setup argument is the request count; any leading arguments
    # (e.g. the batch size) parameterise the scenario and give it its name.
    params = tuple(_parse_args(details)[:-1])

    # Keyed by (tool, metric) to preserve the tool's emitted order while
    # tolerating a metric appearing in more than one profile.
    rows: dict[tuple[str, str], MetricRow] = {}
    for profile in record.get("profiles", []):
        summary = profile.get("summaries", {}).get("total", {}).get("summary", {})
        for tool, _ in _TOOLS:
            entries = summary.get(tool)
            if not entries:
                continue
            for key, entry in entries.items():
                if (tool, key) in rows:
                    continue
                metric = _metric(entry.get("metrics", {}))
                if metric is not None:
                    rows[(tool, key)] = MetricRow(key, _label(tool, key), tool, metric)

    return Benchmark(
        function=function,
        details=details,
        name=_scenario_name(function, params),
        rows=list(rows.values()),
    )


def parse_results(text: str) -> list[Benchmark]:
    benchmarks = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        benchmarks.append(_benchmark(json.loads(line)))
    return benchmarks


@dataclass
class Thresholds:
    instructions: float
    allocations: float
    peak: float


def _grew(metric: Metric, threshold: float, *, absolute: bool) -> bool:
    """Whether ``metric`` grew past ``threshold`` (absolute blocks or percent)."""
    if absolute:
        delta = metric.delta_abs
        return delta is not None and delta > threshold
    if metric.old == 0:
        return metric.new > 0
    pct = metric.delta_pct
    return pct is not None and pct >= threshold


def _shrank(metric: Metric, threshold: float, *, absolute: bool) -> bool:
    """Whether ``metric`` shrank past ``threshold`` (absolute blocks or percent)."""
    if absolute:
        delta = metric.delta_abs
        return delta is not None and delta < -threshold
    pct = metric.delta_pct
    return pct is not None and pct <= -threshold


def _row_status(row: MetricRow, thresholds: Thresholds) -> Optional[str]:
    """Classifies a single metric, or ``None`` if it is not threshold-checked."""
    spec = _THRESHOLDED.get(row.key)
    if spec is None:
        return None
    attribute, absolute = spec
    threshold = getattr(thresholds, attribute)
    if row.metric.old is None:
        return "new"
    if _grew(row.metric, threshold, absolute=absolute):
        return "regression"
    if _shrank(row.metric, threshold, absolute=absolute):
        return "improvement"
    return "ok"


def _statuses(bench: Benchmark, thresholds: Thresholds) -> list[str]:
    return [
        status
        for status in (_row_status(row, thresholds) for row in bench.rows)
        if status is not None
    ]


def is_regression(bench: Benchmark, thresholds: Thresholds) -> bool:
    return "regression" in _statuses(bench, thresholds)


def is_improvement(bench: Benchmark, thresholds: Thresholds) -> bool:
    # A regression in any metric wins over a coincidental improvement in another.
    statuses = _statuses(bench, thresholds)
    return "regression" not in statuses and "improvement" in statuses


def _fmt_value(value: float, is_float: bool) -> str:
    if is_float:
        return f"{value:,.5f}"
    return f"{int(round(value)):,}"


def _fmt_change(metric: Metric) -> str:
    """Formats the change like gungraun: ``+1.23% [+1.01x]`` or ``No change``."""
    if metric.old is None:
        return "n/a"
    if metric.new == metric.old:
        return "No change"
    if metric.old == 0:
        # Percentage and factor are undefined against a zero baseline.
        return "n/a"
    pct = (metric.new - metric.old) / metric.old * 100.0
    if metric.new > metric.old:
        factor = metric.new / metric.old
    elif metric.new != 0:
        factor = -(metric.old / metric.new)
    else:
        return f"{pct:+.2f}%"
    return f"{pct:+.2f}% [{factor:+.2f}x]"


_STATUS_CELL = {
    "regression": "**regression**",
    "improvement": "**improvement**",
    "ok": "ok",
    "new": "new",
    None: "—",
}


def _section(bench: Benchmark, thresholds: Thresholds) -> str:
    heading = f"### `{bench.function}`"
    if bench.details:
        heading += f" — `{bench.details}`"
    parts = [heading, ""]
    for tool, title in _TOOLS:
        rows = [row for row in bench.rows if row.tool == tool]
        if not rows:
            continue
        parts.append(f"**{title}**")
        parts.append("")
        parts.append("| Metric | Pull request | Baseline | Change | Status |")
        parts.append("| :-- | --: | --: | :-- | :-- |")
        for row in rows:
            metric = row.metric
            parts.append(
                "| {label} | {new} | {old} | {change} | {status} |".format(
                    label=row.label,
                    new=_fmt_value(metric.new, metric.is_float),
                    old="n/a"
                    if metric.old is None
                    else _fmt_value(metric.old, metric.is_float),
                    change=_fmt_change(metric),
                    status=_STATUS_CELL[_row_status(row, thresholds)],
                )
            )
        parts.append("")
    return "\n".join(parts).rstrip()


def build_summary(benchmarks: list[Benchmark], thresholds: Thresholds) -> str:
    regressions = sum(is_regression(b, thresholds) for b in benchmarks)
    improvements = sum(is_improvement(b, thresholds) for b in benchmarks)
    lines = [
        "## Driver benchmarks",
        "",
        "Comparison of this pull request against its base, mirroring the "
        "`cargo bench` output. Instructions come from Callgrind; allocated heap "
        "blocks and peak memory from DHAT. Each metric is measured over the "
        "scenario's request loop (setup is excluded by the harness).",
        "",
        f"**{regressions} regression(s), {improvements} improvement(s).** "
        f"Only instructions, allocations (`Total blocks`) and peak memory "
        f"(`At t-gmax bytes`) are checked against thresholds and carry a status; "
        f"other rows are shown for context (`—`).",
        "",
    ]
    for bench in benchmarks:
        lines.append(_section(bench, thresholds))
        lines.append("")
    lines.append(
        f"A metric is flagged as a **regression** when instructions grow by at "
        f"least {thresholds.instructions:g}%, allocations grow by more than "
        f"{thresholds.allocations:g} blocks, or peak memory grows by at least "
        f"{thresholds.peak:g}%, and as an **improvement** when it shrinks by the "
        f"same margin."
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "results",
        help="Path to the JSON-lines benchmark comparison output, or '-' for stdin.",
    )
    parser.add_argument(
        "--summary",
        help="Write the full Markdown benchmark report to this file (default: stdout).",
    )
    parser.add_argument(
        "--instructions-threshold",
        type=float,
        default=5.0,
        help="Minimum instruction-count change (in percent) to flag a "
        "regression or improvement.",
    )
    parser.add_argument(
        "--allocations-threshold",
        type=float,
        default=1.0,
        help="Minimum allocation change (absolute block count) to flag a "
        "regression or improvement.",
    )
    parser.add_argument(
        "--peak-threshold",
        type=float,
        default=5.0,
        help="Minimum peak-memory change (in percent) to flag a regression or "
        "improvement.",
    )
    args = parser.parse_args()

    thresholds = Thresholds(
        instructions=args.instructions_threshold,
        allocations=args.allocations_threshold,
        peak=args.peak_threshold,
    )

    if args.results == "-":
        text = sys.stdin.read()
    else:
        with open(args.results, encoding="utf-8") as handle:
            text = handle.read()

    benchmarks = parse_results(text)
    if not benchmarks:
        print("error: no benchmark results found", file=sys.stderr)
        return 1

    summary = build_summary(benchmarks, thresholds)
    if args.summary:
        with open(args.summary, "w", encoding="utf-8") as handle:
            handle.write(summary)
    else:
        sys.stdout.write(summary)

    regressed = [b for b in benchmarks if is_regression(b, thresholds)]
    improved = [b for b in benchmarks if is_improvement(b, thresholds)]
    has_regressions = bool(regressed)

    for bench in regressed:
        print(f"regression: {bench.name}", file=sys.stderr)
    for bench in improved:
        print(f"improvement: {bench.name}", file=sys.stderr)
    print(
        f"regressions={len(regressed)} improvements={len(improved)}",
        file=sys.stderr,
    )

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as handle:
            handle.write(f"has_regressions={'true' if has_regressions else 'false'}\n")

    # Regressions are reported, not fatal: the workflow decides what to do.
    return 0


if __name__ == "__main__":
    sys.exit(main())
