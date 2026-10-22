#!/usr/bin/env python3
"""Draw a cell's measured tool calls as a Mermaid timing (Gantt) diagram.

    tools/timing-diagram.py RAW_JSONL_GZ [--window S] [--worker N] [--title TEXT]

One row per worker (or only worker N); each call is three bars: acquire (crit), tool (active) and
release (done). Takes the calls that start within the first --window seconds
after warm-up; bars running past the chart's end (window + 25%) are cut there,
and the title says how many. Times are seconds from the first measured call.
"""
import argparse
import gzip
import json
import sys

p = argparse.ArgumentParser()
p.add_argument("raw")
p.add_argument("--window", type=float, default=12)
p.add_argument("--worker", type=int, help="draw only this worker")
p.add_argument("--title", default="")
a = p.parse_args()

calls = [json.loads(l) for l in gzip.open(a.raw, "rt")]
calls = [c for c in calls if not c["warmup"] and c["ok"]]
t0 = min(c["t_start"] for c in calls)
if a.worker is not None:
    calls = [c for c in calls if c["worker"] == a.worker]
end = a.window * 1.25
shown = sorted((c for c in calls if c["t_start"] - t0 < a.window), key=lambda c: (c["worker"], c["t_start"]))
cut = sum(1 for c in shown if c["t_released"] - t0 > end)


def ts(t: float) -> str:
    """Seconds from t0 as a clock time on an arbitrary day (Mermaid needs dates)."""
    t = max(0.0, min(t - t0, end))
    return f"2026-01-01 00:{int(t // 60):02d}:{t % 60:06.3f}"


title = a.title + (f" ({cut} call{'s' if cut != 1 else ''} cut off at {end:g} s)" if cut else "")
out = ["gantt", f"    title {title}", "    dateFormat YYYY-MM-DD HH:mm:ss.SSS",
       "    axisFormat %S s", f"    tickInterval {'1second' if end <= 5 else '2second'}"]
for w in sorted({c["worker"] for c in shown}):
    out.append(f"    section worker {w}")
    for c in (c for c in shown if c["worker"] == w):
        for label, tag, s, e in (("acquire", "crit", c["t_start"], c["t_acquired"]),
                                 ("tool", "active", c["t_acquired"], c["t_tool_done"]),
                                 ("release", "done", c["t_tool_done"], c["t_released"])):
            if s - t0 < end:
                out.append(f"    {label} :{tag}, {ts(s)}, {ts(max(e, s + 0.001))}")
print("\n".join(out))
