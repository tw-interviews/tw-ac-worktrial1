#!/usr/bin/env python3
"""Build a self-contained HTML results page from benchmark results.

    tools/report.py run RESULTS_DIR OUT.html --title T --setup "..."
        One run: charts for whichever cells it has, and every cell in a table.
    tools/report.py compare DIR_A DIR_B OUT.html --labels "A,B" --title T --setup "..."
        Two runs side by side, over the cells both have.

Reads RESULTS_DIR/summary.tsv, plus the harness arguments each cell ran with
from summary.jsonl. Cell names follow run-matrix.sh:
<closed-loop|burst|open-loop>_<sandboxmanager|agent-sandbox-cold|agent-sandbox-poolN|reuse>_<wNN|nNN|rX>.
The page draws its charts in the browser from the embedded rows; no network.
"""
import argparse
import csv
import html
import json
import re
from pathlib import Path


def describe(mode, src, pool, args):
    """The test behind a cell, in words, from its harness arguments."""
    a = dict(zip(args[::2], args[1::2]))
    window = f"{a.get('--duration', '180')} s measured after a {a.get('--warmup', '30')} s warm-up"
    if mode == "closed-loop":
        w = int(a.get("--workers", 0))
        what = [f"Closed loop: {w} worker{'s' if w != 1 else ''}, each starting its next call when the last ends", window]
    elif mode == "burst":
        what = [f"Burst: {a.get('--calls')} calls started at once"]
    else:
        what = [f"Open loop: Poisson arrivals at {a.get('--rate')} calls/s, at most {a.get('--max-inflight', '64')} in flight", window]
    if src.startswith("agent-sandbox"):
        what.append(f"warm pool filled to {pool} first" if pool else "no warm pool (cold start)")
    return "; ".join(what)


def load(d: Path):
    args = {}
    if (d / "summary.jsonl").exists():
        for line in open(d / "summary.jsonl"):
            j = json.loads(line)
            args[j["cell"]] = j.get("args") or ""
    rows = []
    for r in csv.DictReader(open(d / "summary.tsv"), delimiter="\t"):
        m = re.fullmatch(r"(closed-loop|burst|open-loop)_(.+)_([wnr])([\d.]+)", r["cell"])
        if not m:
            continue
        mode, src, kind, val = m.groups()
        num = lambda k: float(r[k]) if r.get(k) not in (None, "", "None") else None
        rows.append({"cell": r["cell"], "mode": mode, "src": src, "x": float(val), "pool": int(r["pool_depth"]),
                     "calls": int(r["calls"]), "errors": int(r["errors"]), "cps": num("tool_calls_per_s"),
                     "acq50": num("acquire_p50"), "acq95": num("acquire_p95"), "e2e50": num("e2e_p50"),
                     "e2e95": num("e2e_p95"), "stopped": r.get("stopped") or ""})
        if r["cell"] in args:
            rows[-1]["test"] = describe(mode, src, rows[-1]["pool"], args[r["cell"]].split())
            rows[-1]["cmd"] = "toolcall_bench.py run " + args[r["cell"]]
    return rows


STYLE = """
  /* Layout: a benchmark report. Headline figures, one panel per experiment (chart + one line of reading), every cell in a table. Series colors: the dataviz reference palette in fixed slot order, one color per sandbox source on every page. */
  :root {
    --bg: #f4f5f7; --panel: #fcfcfb; --ink: #0b0b0b; --muted: #52514e; --rule: #dcdfe5; --grid: #ebedf0;
    --band: #fbefe2; --band-ink: #8f4a12;
    --s-sandboxmanager: #2a78d6; --s-agent-sandbox-cold: #eb6834; --s-agent-sandbox-pool32: #1baf7a;
    --s-agent-sandbox-pool16: #eda100; --s-agent-sandbox-pool8: #e87ba4; --s-reuse: #008300; --s-agent-sandbox-pool50: #6250d6;
    --font-sans: "IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
    --font-mono: "IBM Plex Mono", ui-monospace, "SF Mono", Menlo, monospace;
  }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) {
      color-scheme: dark; --bg: #121211; --panel: #1a1a19; --ink: #f0efec; --muted: #c3c2b7; --rule: #33332f; --grid: #2a2a27;
      --band: #2e2216; --band-ink: #f3b77a;
      --s-sandboxmanager: #3987e5; --s-agent-sandbox-cold: #d95926; --s-agent-sandbox-pool32: #199e70;
      --s-agent-sandbox-pool16: #c98500; --s-agent-sandbox-pool8: #d55181; --s-reuse: #008300; --s-agent-sandbox-pool50: #9085e9;
    }
  }
  :root[data-theme="dark"] {
    color-scheme: dark; --bg: #121211; --panel: #1a1a19; --ink: #f0efec; --muted: #c3c2b7; --rule: #33332f; --grid: #2a2a27;
    --band: #2e2216; --band-ink: #f3b77a;
    --s-sandboxmanager: #3987e5; --s-agent-sandbox-cold: #d95926; --s-agent-sandbox-pool32: #199e70;
    --s-agent-sandbox-pool16: #c98500; --s-agent-sandbox-pool8: #d55181; --s-reuse: #008300; --s-agent-sandbox-pool50: #9085e9;
  }
  body { background: var(--bg); color: var(--ink); font-family: var(--font-sans); font-size: 15px; line-height: 1.55; }
  .wrap { max-width: 1060px; margin: 0 auto; padding-block: 32px 64px;
    padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px));
    display: grid; gap: 28px; }
  header { display: grid; gap: 8px; }
  .eyebrow { font-family: var(--font-mono); font-size: 12px; letter-spacing: 0.06em; text-transform: uppercase; color: var(--muted); }
  h1 { font-size: 30px; line-height: 1.2; margin: 0; font-weight: 600; text-wrap: balance; }
  h2 { font-size: 17px; line-height: 1.3; margin: 0; font-weight: 600; text-wrap: balance; }
  p { margin: 0; max-width: 74ch; }
  .lede { color: var(--muted); }
  .tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
  .tile { background: var(--panel); border: 1px solid var(--rule); border-radius: 8px; padding: 14px 16px; display: grid; gap: 4px; align-content: start; }
  .tile .k { font-size: 13px; color: var(--muted); }
  .tile .v { font-family: var(--font-mono); font-size: 24px; font-weight: 500; font-variant-numeric: tabular-nums; }
  .tile .s { font-size: 13px; color: var(--muted); }
  .grid2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(440px, 1fr)); gap: 16px; }
  @media (max-width: 520px) { .grid2 { grid-template-columns: 1fr; } }
  .panel { background: var(--panel); border: 1px solid var(--rule); border-radius: 10px; padding: 18px; display: grid; gap: 10px; align-content: start; min-width: 0; }
  .panel[hidden] { display: none; }
  h3.sub { font-size: 14px; font-weight: 600; margin: 0 0 6px; }
  /* Phase timelines in --section fragments: neutral fills, every segment labelled. */
  .phases { display: grid; gap: 10px; }
  .phase-row { display: grid; grid-template-columns: minmax(0, 150px) 1fr; gap: 12px; align-items: center; font-size: 13px; }
  .phase-bar { display: flex; gap: 2px; height: 26px; min-width: 0; }
  .phase-bar span { display: flex; align-items: center; justify-content: center; border-radius: 4px; font-size: 11px; white-space: nowrap; overflow: hidden; min-width: 0; }
  .ph-warm { background: var(--grid); color: var(--muted); }
  .ph-meas { background: var(--muted); color: var(--panel); }
  .ph-drain { background: var(--band); color: var(--band-ink); }
  .ph-cool { border: 1.5px dashed var(--rule); color: var(--muted); }
  .read { font-size: 14px; color: var(--muted); }
  .chart { position: relative; }
  .chart svg { display: block; width: 100%; height: auto; overflow: visible; }
  svg text { font-family: var(--font-sans); }
  .ax { fill: var(--muted); font-size: 11px; font-family: var(--font-mono); }
  .axt { fill: var(--muted); font-size: 11.5px; }
  .gl { stroke: var(--grid); stroke-width: 1; }
  .base { stroke: var(--rule); stroke-width: 1; }
  .lbl { fill: var(--ink); font-size: 11.5px; font-family: var(--font-mono); }
  .band { fill: var(--band); }
  .bandt { fill: var(--band-ink); font-size: 11px; font-weight: 600; }
  .ref { stroke: var(--muted); stroke-width: 1.5; stroke-dasharray: 4 4; fill: none; }
  .ref.half { stroke-dasharray: 1 4; }
  .reft { fill: var(--muted); font-size: 11px; }
  .legend { display: flex; flex-wrap: wrap; gap: 6px 16px; font-size: 13px; color: var(--muted); }
  .legend span { display: inline-flex; align-items: center; gap: 6px; }
  .legend i { width: 16px; height: 0; border-top: 3px solid; display: inline-block; }
  .legend i.dash { border-top-style: dashed; border-top-width: 2px; }
  .tip { position: absolute; pointer-events: none; background: var(--panel); color: var(--ink); border: 1px solid var(--rule);
    border-radius: 6px; padding: 6px 9px; font-size: 12.5px; box-shadow: 0 4px 14px rgb(0 0 0 / 0.12); white-space: nowrap; z-index: 2; display: none; }
  .tip b { font-weight: 600; }
  .tip .m { font-family: var(--font-mono); font-variant-numeric: tabular-nums; }
  .table-scroll { overflow-x: auto; background: var(--panel); border: 1px solid var(--rule); border-radius: 10px; }
  table { border-collapse: collapse; width: 100%; min-width: 820px; font-size: 13.5px; }
  th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid var(--rule); }
  td.n, th.n { text-align: right; font-family: var(--font-mono); font-variant-numeric: tabular-nums; }
  th { font-size: 11.5px; font-weight: 500; color: var(--muted); letter-spacing: 0.04em; text-transform: uppercase; background: var(--panel); }
  tr.group td { font-weight: 600; background: var(--bg); }
  tr:last-child td { border-bottom: 0; }
  .sw { display: inline-block; width: 9px; height: 9px; border-radius: 2px; margin-right: 8px; }
  ul { margin: 0; padding-left: 20px; display: grid; gap: 6px; }
  li { font-size: 14px; max-width: 82ch; }
  .note { font-size: 13px; color: var(--muted); }
  td.test { min-width: 280px; max-width: 420px; font-size: 13px; }
  td.test code { display: block; margin-top: 4px; font-family: var(--font-mono); font-size: 11.5px; color: var(--muted); overflow-wrap: anywhere; }
  .tip .tt { margin-top: 6px; max-width: 260px; color: var(--muted); font-size: 12px; white-space: normal; }
"""

# Shared chart code. RUNS = [{label, dash, rows}]; one run draws solid lines,
# a comparison draws the second run dashed.
SCRIPT = r"""
const NAMES = {"sandboxmanager": "SandboxManager", "agent-sandbox-cold": "agent-sandbox, cold start",
  "agent-sandbox-pool50": "agent-sandbox, pool 50", "agent-sandbox-pool32": "agent-sandbox, pool 32", "agent-sandbox-pool16": "agent-sandbox, pool 16",
  "agent-sandbox-pool8": "agent-sandbox, pool 8", "reuse": "agent-sandbox, reused"};
const ORDER = Object.keys(NAMES);
const color = src => `var(--s-${src})`;
const NS = "http://www.w3.org/2000/svg";
const el = (n, a = {}, t) => { const e = document.createElementNS(NS, n); for (const k in a) e.setAttribute(k, a[k]); if (t != null) e.textContent = t; return e; };
const fmt = v => v == null ? "–" : (Math.round(v * 100) / 100).toString();
const cells = (run, mode, src) => run.rows.filter(r => r.mode === mode && r.src === src).sort((a, b) => a.x - b.x);
const name = (src, run) => NAMES[src] + (RUNS.length > 1 ? ` · ${run.label}` : "");

function tipLines(title, pairs, test) {
  const b = document.createElement("b"); b.textContent = title; const out = [b];
  for (const [k, v] of pairs) { const d = document.createElement("div"); const m = document.createElement("span"); m.className = "m"; m.textContent = v; d.append(k + ": ", m); out.push(d); }
  if (test) { const d = document.createElement("div"); d.className = "tt"; d.textContent = test; out.push(d); }
  return out;
}
function frame(id, { xs, xLabel, yMax, yLabel, yTicks, logX, H = 260 }) {
  const host = document.getElementById(id);
  const W = 520, M = { l: 48, r: 16, t: 14, b: 40 }, iw = W - M.l - M.r, ih = H - M.t - M.b;
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img" });
  const lx = v => logX ? Math.log2(v) : v, x0 = lx(xs[0]), x1 = lx(xs[xs.length - 1]);
  const x = v => M.l + ((lx(v) - x0) / ((x1 - x0) || 1)) * iw, y = v => M.t + ih - (v / yMax) * ih;
  for (const t of yTicks) { svg.append(el("line", { class: "gl", x1: M.l, x2: W - M.r, y1: y(t), y2: y(t) })); svg.append(el("text", { class: "ax", x: M.l - 8, y: y(t) + 4, "text-anchor": "end" }, String(t))); }
  svg.append(el("line", { class: "base", x1: M.l, x2: W - M.r, y1: y(0), y2: y(0) }));
  for (const v of xs) svg.append(el("text", { class: "ax", x: x(v), y: H - M.b + 16, "text-anchor": "middle" }, String(v)));
  svg.append(el("text", { class: "axt", x: M.l + iw / 2, y: H - 6, "text-anchor": "middle" }, xLabel));
  svg.append(el("text", { class: "axt", x: 12, y: M.t + ih / 2, transform: `rotate(-90 12 ${M.t + ih / 2})`, "text-anchor": "middle" }, yLabel));
  host.append(svg);
  const tip = document.createElement("div"); tip.className = "tip"; host.append(tip);
  const show = (e, lines) => { tip.replaceChildren(...lines); tip.style.display = "block"; const r = host.getBoundingClientRect(), tw = tip.offsetWidth;
    let left = e.clientX - r.left + 12; if (left + tw > r.width) left = e.clientX - r.left - tw - 12; tip.style.left = Math.max(0, left) + "px"; tip.style.top = (e.clientY - r.top - 10) + "px"; };
  return { svg, x, y, M, W, H, iw, show, hide: () => { tip.style.display = "none"; } };
}
function lines(f, series, val, xName, pairs) {
  for (const s of series) {
    const pts = s.rows.filter(r => val(r) != null).map(r => [f.x(r.x), f.y(val(r)), r]);
    if (!pts.length) continue;
    f.svg.append(el("path", { d: pts.map((p, i) => (i ? "L" : "M") + p[0] + " " + p[1]).join(" "), fill: "none", stroke: color(s.src), "stroke-width": 2, "stroke-dasharray": s.dash ? "6 4" : "none" }));
    for (const [px, py, r] of pts) {
      f.svg.append(el("circle", { cx: px, cy: py, r: 4, fill: s.dash ? "var(--panel)" : color(s.src), stroke: s.dash ? color(s.src) : "var(--panel)", "stroke-width": 2 }));
      const hit = el("circle", { cx: px, cy: py, r: 11, fill: "transparent" });
      hit.addEventListener("mousemove", e => f.show(e, tipLines(`${s.name} · ${xName(r)}`, pairs ? pairs(r) : [["calls/s", fmt(r.cps)], ["acquire p50", fmt(r.acq50) + " s"], ["e2e p95", fmt(r.e2e95) + " s"]], r.test)));
      hit.addEventListener("mouseleave", f.hide); f.svg.append(hit);
    }
  }
}
function legend(id, series) {
  const box = document.getElementById(id);
  for (const s of series) { const sp = document.createElement("span"); const i = document.createElement("i"); i.style.borderTopColor = color(s.src); if (s.dash) i.className = "dash"; sp.append(i, s.name); box.append(sp); }
}
function seriesFor(mode, srcs) {
  const out = [];
  for (const run of RUNS) for (const src of srcs) { const rows = cells(run, mode, src); if (rows.length) out.push({ src, rows, dash: run.dash, name: name(src, run) }); }
  return out;
}
const niceMax = v => { const s = [1, 2, 2.5, 3, 5, 10, 12, 15, 20, 25, 30, 40, 50, 60, 80, 100, 110, 150, 200]; return s.find(x => x >= v * 1.05) || Math.ceil(v * 1.1); };
const ticks = m => { const step = m <= 3 ? 1 : m <= 12 ? 2 : m <= 30 ? 5 : m <= 60 ? 10 : m <= 120 ? 25 : 50; const t = []; for (let v = 0; v <= m + 1e-9; v += step) t.push(+v.toFixed(2)); return t; };
function hidePanel(id) { document.getElementById(id).closest(".panel").hidden = true; }

function closedChart(id, legendId, val, yLabel, srcs, band) {
  const series = seriesFor("closed-loop", srcs);
  if (!series.length) return hidePanel(id);
  const xs = [...new Set(series.flatMap(s => s.rows.map(r => r.x)))].sort((a, b) => a - b);
  const yMax = niceMax(Math.max(...series.flatMap(s => s.rows.map(val)).filter(v => v != null), band ? 2.5 : 0));
  const f = frame(id, { xs, logX: true, xLabel: "workers (log scale)", yMax, yLabel, yTicks: ticks(yMax) });
  if (band) { f.svg.insertBefore(el("rect", { class: "band", x: f.M.l, width: f.iw, y: f.y(2.5), height: f.y(2) - f.y(2.5) }), f.svg.firstChild); f.svg.append(el("text", { class: "bandt", x: f.M.l + 6, y: f.y(2.5) - 5 }, "observed cluster ceiling ~2–2.5/s")); }
  lines(f, series, val, r => `${r.x} workers`);
  legend(legendId, series);
}
function reuseChart(id) {
  const series = seriesFor("closed-loop", ["reuse"]);
  if (!series.length) return hidePanel(id);
  const xs = series[0].rows.map(r => r.x), yMax = niceMax(Math.max(...xs.map(w => w / 0.3)));
  const f = frame(id, { xs, logX: true, xLabel: "workers (log scale)", yMax, yLabel: "tool calls / s", yTicks: ticks(yMax) });
  f.svg.append(el("path", { class: "ref", d: xs.map((w, i) => (i ? "L" : "M") + f.x(w) + " " + f.y(w / 0.3)).join(" ") }));
  lines(f, series, r => r.cps, r => `${r.x} workers`);
}
function burstChart(id) {
  const bars = [];
  for (const run of RUNS) for (const r of run.rows.filter(r => r.mode === "burst").sort((a, b) => ORDER.indexOf(a.src) - ORDER.indexOf(b.src) || a.x - b.x))
    bars.push({ r, run, label: `${r.x} calls · ${NAMES[r.src]}` + (RUNS.length > 1 ? ` · ${run.label}` : "") });
  if (!bars.length) return hidePanel(id);
  const host = document.getElementById(id), W = 520, rowH = 34, M = { l: 230, r: 64, t: 6, b: 30 }, H = M.t + bars.length * rowH + M.b;
  const max = niceMax(Math.max(...bars.map(b => b.r.cps))), x = v => M.l + (v / max) * (W - M.l - M.r);
  const svg = el("svg", { viewBox: `0 0 ${W} ${H}`, role: "img" });
  for (const t of ticks(max)) { svg.append(el("line", { class: "gl", x1: x(t), x2: x(t), y1: M.t, y2: H - M.b })); svg.append(el("text", { class: "ax", x: x(t), y: H - M.b + 16, "text-anchor": "middle" }, String(t))); }
  svg.append(el("text", { class: "axt", x: x(max / 2), y: H - 2, "text-anchor": "middle" }, "tool calls / s"));
  host.append(svg); const tip = document.createElement("div"); tip.className = "tip"; host.append(tip);
  bars.forEach((b, i) => {
    const yy = M.t + i * rowH + 7, r = b.r;
    svg.append(el("text", { class: "axt", x: M.l - 10, y: yy + 13, "text-anchor": "end" }, b.label));
    svg.append(el("rect", { x: M.l, y: yy, width: Math.max(4, x(r.cps) - M.l), height: 18, rx: 4, fill: b.run.dash ? "var(--panel)" : color(r.src), stroke: color(r.src), "stroke-width": b.run.dash ? 2 : 0, "stroke-dasharray": b.run.dash ? "5 3" : "none" }));
    svg.append(el("text", { class: "lbl", x: x(r.cps) + 6, y: yy + 13 }, `${fmt(r.cps)}/s`));
    const hit = el("rect", { x: 0, y: yy - 6, width: W, height: rowH - 4, fill: "transparent" });
    hit.addEventListener("mousemove", e => { tip.replaceChildren(...tipLines(b.label, [["calls/s", fmt(r.cps)], ["acquire p50", fmt(r.acq50) + " s"], ["e2e p95", fmt(r.e2e95) + " s"]], r.test));
      tip.style.display = "block"; const bb = host.getBoundingClientRect(); tip.style.left = Math.min(e.clientX - bb.left + 12, bb.width - tip.offsetWidth) + "px"; tip.style.top = (e.clientY - bb.top - 10) + "px"; });
    hit.addEventListener("mouseleave", () => { tip.style.display = "none"; }); svg.append(hit);
  });
}
function openChart(id, legendId) {
  const series = seriesFor("open-loop", ORDER);
  if (!series.length) return hidePanel(id);
  const xMax = Math.max(...series.flatMap(s => s.rows.map(r => r.x))), yMax = niceMax(Math.max(xMax, ...series.flatMap(s => s.rows.map(r => r.cps)))), m = Math.max(xMax, yMax);
  const xs = [...new Set(series.flatMap(s => s.rows.map(r => r.x)))].sort((a, b) => a - b);
  const f = frame(id, { xs: [0, ...xs], xLabel: "offered rate (calls / s)", yMax, yLabel: "delivered (calls / s)", yTicks: ticks(yMax) });
  f.svg.append(el("path", { class: "ref", d: `M${f.x(0)} ${f.y(0)} L${f.x(xMax)} ${f.y(Math.min(xMax, yMax))}` }));
  f.svg.append(el("path", { class: "ref half", d: `M${f.x(0)} ${f.y(0)} L${f.x(xMax)} ${f.y(xMax / 2)}` }));
  f.svg.append(el("text", { class: "reft", x: f.x(xMax) - 4, y: f.y(xMax / 2) + 14, "text-anchor": "end" }, "50% of offered"));
  lines(f, series, r => r.cps, r => `offered ${r.x}/s`);
  legend(legendId, series);
}
function errChart(id, legendId) {
  const series = seriesFor("open-loop", ORDER).map(s => ({ ...s, rows: s.rows.map(r => ({ ...r, err: r.calls ? 100 * r.errors / r.calls : 0 })) }));
  if (!series.length) return hidePanel(id);
  const xs = [...new Set(series.flatMap(s => s.rows.map(r => r.x)))].sort((a, b) => a - b);
  const yMax = niceMax(Math.max(6, ...series.flatMap(s => s.rows.map(r => r.err))));
  const f = frame(id, { xs: [0, ...xs], xLabel: "offered rate (calls / s)", yMax, yLabel: "calls failed or dropped (%)", yTicks: ticks(yMax) });
  f.svg.append(el("path", { class: "ref", d: `M${f.x(0)} ${f.y(5)} L${f.x(xs[xs.length - 1])} ${f.y(5)}` }));
  f.svg.append(el("text", { class: "reft", x: f.M.l + 6, y: f.y(5) - 5 }, "stop rule: over 5% in the last 30 s"));
  lines(f, series, r => r.err, r => `offered ${r.x}/s`, r => [["errors", `${r.errors} of ${r.calls} (${fmt(r.err)}%)`], ["delivered", fmt(r.cps) + "/s"]]);
  legend(legendId, series);
}
function table(id) {
  const tb = document.getElementById(id);
  const head = tb.closest("table").querySelector("thead tr");
  if (RUNS.length > 1) { const th = document.createElement("th"); th.textContent = "Run"; head.insertBefore(th, head.children[1]); }
  for (const [title, mode] of [["Closed loop", "closed-loop"], ["Bursts", "burst"], ["Open loop (rate sweeps)", "open-loop"]]) {
    const rows = RUNS.flatMap(run => run.rows.filter(r => r.mode === mode).map(r => [run, r]))
      .sort((a, b) => ORDER.indexOf(a[1].src) - ORDER.indexOf(b[1].src) || a[1].x - b[1].x || RUNS.indexOf(a[0]) - RUNS.indexOf(b[0]));
    if (!rows.length) continue;
    const g = document.createElement("tr"); g.className = "group"; const gt = document.createElement("td"); gt.colSpan = RUNS.length > 1 ? 12 : 11; gt.textContent = title; g.append(gt); tb.append(g);
    for (const [run, r] of rows) {
      const tr = document.createElement("tr"), c = document.createElement("td"), sw = document.createElement("span");
      sw.className = "sw"; sw.style.background = color(r.src); c.append(sw, r.cell); tr.append(c);
      if (RUNS.length > 1) { const t = document.createElement("td"); t.textContent = run.label; tr.append(t); }
      const tc = document.createElement("td"); tc.className = "test"; tc.append(r.test || "–");
      if (r.cmd) { const code = document.createElement("code"); code.textContent = r.cmd; tc.append(code); }
      tr.append(tc);
      for (const v of [r.pool, r.calls, r.errors, fmt(r.cps), fmt(r.acq50), fmt(r.acq95), fmt(r.e2e50), fmt(r.e2e95)]) { const td = document.createElement("td"); td.className = "n"; td.textContent = String(v); tr.append(td); }
      const st = document.createElement("td"); st.textContent = r.stopped ? "yes" : ""; st.title = r.stopped; tr.append(st);
      tb.append(tr);
    }
  }
}
const CREATING = ORDER.filter(s => s !== "reuse");
closedChart("c-throughput", "l-throughput", r => r.cps, "tool calls / s", CREATING, true);
closedChart("c-acquire", "l-acquire", r => r.acq50, "acquire p50 (s)", CREATING, false);
reuseChart("c-reuse");
burstChart("c-burst");
openChart("c-open", "l-open");
errChart("c-err", "l-err");
closedChart("c-tail", "l-tail", r => r.e2e95, "end-to-end p95 (s)", ["sandboxmanager", "agent-sandbox-cold"], false);
table("rows");
"""

BODY = """
<div class="wrap">
  <header>
    <div class="eyebrow">{eyebrow}</div>
    <h1>{title}</h1>
    <p class="lede">{lede}</p>
  </header>
  <div class="tiles">{tiles}</div>
  <div class="grid2">
    <section class="panel"><h2>Tool calls per second as workers grow</h2><div class="chart" id="c-throughput"></div><div class="legend" id="l-throughput"></div>
      <p class="read">One sandbox per call: every arm that creates sandboxes is capped by how fast the cluster turns pods over.</p></section>
    <section class="panel"><h2>Time to get a sandbox (acquire p50)</h2><div class="chart" id="c-acquire"></div><div class="legend" id="l-acquire"></div>
      <p class="read">How long a worker waits from asking for a sandbox until it can run a tool.</p></section>
    <section class="panel"><h2>Reused sandboxes against the ideal</h2><div class="chart" id="c-reuse"></div>
      <div class="legend"><span><i style="border-top-color:var(--s-reuse)"></i>agent-sandbox, reused</span><span><i class="dash" style="border-top-color:var(--muted)"></i>ideal: workers ÷ 0.3 s</span></div>
      <p class="read">With nothing created per call, throughput tracks the ideal: creating sandboxes is the whole cost.</p></section>
    <section class="panel"><h2>Bursts: N tool calls fired at once</h2><div class="chart" id="c-burst"></div>
      <p class="read">How fast a fan-out of calls is absorbed{burst_note}.</p></section>
    <section class="panel"><h2>Sustained load: delivered vs offered rate</h2><div class="chart" id="c-open"></div><div class="legend" id="l-open"></div>
      <p class="read">Each sweep raises the offered rate until delivered falls below 50% of it (sweeps run before that rule stopped at 90%, or at a stop rule).</p></section>
    <section class="panel"><h2>Error rate as offered load rises</h2><div class="chart" id="c-err"></div><div class="legend" id="l-err"></div>
      <p class="read">Failed calls plus arrivals dropped at the in-flight cap, as a share of the cell's calls. A cell stops issuing calls once 5% of the last 30 s failed, so this reads as "where the stop rule fired", not how high errors would climb.</p></section>
    <section class="panel"><h2>Tail latency (end-to-end p95)</h2><div class="chart" id="c-tail"></div><div class="legend" id="l-tail"></div>
      <p class="read">Long tails here are pod-IP retries on the test nodes, not tool time.</p></section>
  </div>
{sections}  <section class="panel"><h2>Reading it</h2><ul>{notes}</ul></section>
  <section style="display:grid;gap:12px">
    <h2>Every cell</h2>
    <div class="table-scroll"><table>
      <thead><tr><th>Cell</th><th>Test</th><th class="n">Pool</th><th class="n">Calls</th><th class="n">Errors</th><th class="n">Calls/s</th><th class="n">Acquire p50</th><th class="n">Acquire p95</th><th class="n">E2E p50</th><th class="n">E2E p95</th><th>Stopped</th></tr></thead>
      <tbody id="rows"></tbody>
    </table></div>
    <p class="note">{source} Times in seconds. "Test" is what the cell ran: in words, then the harness command (run inside the loadgen pod by <code>harness/run-matrix.sh</code>, which first fills or drains the warm pool to the cell's depth). "Stopped": a stop rule ended the cell early (hover for the reason).</p>
  </section>
</div>
"""


def page(title, eyebrow, lede, tiles, notes, runs, source, burst_note="", sections=()):
    head = (f"<title>{html.escape(title)}</title>\n<meta name=\"claude:viewport-fit-sides\" content=\"cover\">\n"
            '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
            '<link rel="preconnect" href="https://fonts.googleapis.com">\n<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>\n'
            '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap">\n'
            f"<style>{STYLE}</style>\n")
    tile_html = "".join(f'<div class="tile"><span class="k">{html.escape(k)}</span><span class="v">{html.escape(v)}</span><span class="s">{html.escape(s)}</span></div>' for k, v, s in tiles)
    body = BODY.format(eyebrow=html.escape(eyebrow), title=html.escape(title), lede=html.escape(lede), tiles=tile_html,
                       notes="".join(f"<li>{html.escape(n)}</li>" for n in notes), source=html.escape(source), burst_note=html.escape(burst_note),
                       sections="".join(f'  <section class="panel">{open(f).read()}</section>\n' for f in sections))
    data = "const RUNS = " + json.dumps(runs) + ";\n"
    return head + body + "<script>\n" + data + SCRIPT + "</script>\n"


def get(rows, cell):
    return next((r for r in rows if r["cell"] == cell), None)


NAMES = {"sandboxmanager": "SandboxManager", "agent-sandbox-cold": "agent-sandbox, cold start", "reuse": "agent-sandbox, reused",
         **{f"agent-sandbox-pool{d}": f"agent-sandbox, pool {d}" for d in (8, 16, 32, 50)}}


def tiles_for(rows):
    t = []
    c1, a1 = get(rows, "closed-loop_agent-sandbox-cold_w01"), get(rows, "closed-loop_sandboxmanager_w01")
    if c1 and a1:
        t.append(("Acquire p50 at 1 worker", f"{a1['acq50']:.2f} s · {c1['acq50']:.2f} s", "SandboxManager · agent-sandbox cold start"))
    creating = [r["cps"] for r in rows if r["mode"] == "closed-loop" and r["src"] != "reuse"]
    if creating:
        t.append(("Best one-sandbox-per-call throughput", f"{max(creating):.2f}/s", "highest closed-loop rate of any arm that creates sandboxes"))
    r32 = get(rows, "closed-loop_reuse_w32")
    if r32:
        t.append(("Reused sandboxes, 32 workers", f"{r32['cps']:.1f}/s", "no sandbox created per call"))
    bp, b0 = get(rows, "burst_agent-sandbox-pool32_n32"), get(rows, "burst_agent-sandbox-cold_n32")
    if bp and b0:
        t.append(("32-call burst: pool of 32 vs none", f"{bp['cps']:.1f}/s · {b0['cps']:.1f}/s", f"acquire p50 {bp['acq50']:.2f} s vs {b0['acq50']:.2f} s"))
    opened = [r for r in rows if r["mode"] == "open-loop" and r["cps"] is not None]
    if opened and not t:
        best = max(opened, key=lambda r: r["cps"])
        t.append(("Highest delivered rate", f"{best['cps']:.2f}/s", f"{NAMES.get(best['src'], best['src'])}, offered {best['x']:g}/s"))
        for src in sorted({r["src"] for r in opened}):
            half = next((r for r in sorted(opened, key=lambda r: r["x"]) if r["src"] == src and r["cps"] < 0.5 * r["x"]), None)
            if half:
                t.append((f"Below 50%: {NAMES.get(src, src)}", f"{half['x']:g}/s offered", f"delivered {half['cps']:.2f}/s ({100 * half['cps'] / half['x']:.0f}%), errors {half['errors']}/{half['calls']}"))
    return t


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run"); r.add_argument("dir"); r.add_argument("out")
    c = sub.add_parser("compare"); c.add_argument("a"); c.add_argument("b"); c.add_argument("out"); c.add_argument("--labels", required=True)
    for p in (r, c):
        p.add_argument("--title", required=True); p.add_argument("--setup", required=True)
        p.add_argument("--lede", default=""); p.add_argument("--note", action="append", default=[])
        p.add_argument("--section", action="append", default=[], help="HTML fragment file, shown as a panel before the notes")
    a = ap.parse_args()
    if a.cmd == "run":
        rows = load(Path(a.dir))
        runs = [{"label": "", "dash": False, "rows": rows}]
        eyebrow = f"{a.setup} · {len(rows)} cells · {sum(x['errors'] for x in rows)} errors"
        out = page(a.title, eyebrow, a.lede, tiles_for(rows), a.note, runs, f"Source: {Path(a.dir).name}/summary.tsv.", sections=a.section)
    else:
        la, lb = a.labels.split(",")
        ra, rb = load(Path(a.a)), load(Path(a.b))
        common = {x["cell"] for x in ra} & {x["cell"] for x in rb}
        ra, rb = [x for x in ra if x["cell"] in common], [x for x in rb if x["cell"] in common]
        runs = [{"label": la, "dash": True, "rows": ra}, {"label": lb, "dash": False, "rows": rb}]
        tiles = []
        for cell, what in [("closed-loop_agent-sandbox-cold_w01", "agent-sandbox cold acquire p50, 1 worker"),
                           ("closed-loop_sandboxmanager_w01", "SandboxManager acquire p50, 1 worker")]:
            x, y = get(ra, cell), get(rb, cell)
            if x and y:
                tiles.append((what, f"{x['acq50']:.2f} → {y['acq50']:.2f} s", f"{la} → {lb}"))
        for cell, what in [("burst_agent-sandbox-pool32_n32", "32-call burst, pool of 32"), ("burst_agent-sandbox-cold_n32", "32-call burst, no pool")]:
            x, y = get(ra, cell), get(rb, cell)
            if x and y:
                tiles.append((what, f"{x['cps']:.1f} → {y['cps']:.1f}/s", f"{la} → {lb}"))
        eyebrow = f"{a.setup} · {len(common)} cells in both runs"
        out = page(a.title, eyebrow, a.lede, tiles, a.note, runs,
                   f"Sources: {Path(a.a).name}/summary.tsv (dashed) and {Path(a.b).name}/summary.tsv (solid).", " in each run (outlined bars: " + la + ")")
    Path(a.out).write_text(out)
    print(f"wrote {a.out}")


if __name__ == "__main__":
    main()
