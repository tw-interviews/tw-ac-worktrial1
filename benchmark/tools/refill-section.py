#!/usr/bin/env python3
"""Build the "How fast the warm pool refills" panel from pool-refill experiments.

    tools/refill-section.py REFILL_DIR OUT.html

REFILL_DIR holds what the experiments recorded (pool depth 50, warm pool polled
about once a second):
  cold-fill.csv           pool set from 0 to 50 on a quiet cluster
  burst-refill.csv        all 50 pooled sandboxes claimed at once
  rateR-pool.csv          open-loop arrivals at R calls/s, pool level over time
  rateR-calls.jsonl.gz    ... and the harness's per-call records for that run
Pool CSVs: [epoch,] t, pool_ready, pods_running, pods_pending, pods_terminating.

Refill rate per 10 s bin = calls served from the pool in the bin (acquire under
0.5 s) + the change in ready sandboxes across the bin. The output is an HTML
fragment for `report.py run --section`; its charts use the report's own helpers.
"""
import csv
import gzip
import json
import re
import sys
from pathlib import Path

POOLED = 0.5  # an acquire faster than this came from the pool
d, out = Path(sys.argv[1]), Path(sys.argv[2])
rows = lambda f: list(csv.DictReader(open(d / f)))


def level_at(pool, e):
    """Ready sandboxes at epoch e (last sample at or before it)."""
    lvl = int(pool[0]["pool_ready"])
    for p in pool:
        if float(p["epoch"]) > e:
            break
        lvl = int(p["pool_ready"])
    return lvl


# Quiet fill and single burst: time from first change until the pool is back to 50.
fill = rows("cold-fill.csv")
fill_s = next(float(p["t"]) for p in fill if int(p["pool_ready"]) >= 50)
burst = rows("burst-refill.csv")
b0 = next(float(p["t"]) for p in burst if int(p["pool_ready"]) < 50 or int(p["pods_pending"]) > 0)
burst_low = min(int(p["pool_ready"]) for p in burst)
burst_s = next(float(p["t"]) for p in burst if float(p["t"]) > b0 and int(p["pool_ready"]) >= 50) - b0

runs, metrics = [], []
for f in sorted(d.glob("rate*-calls.jsonl.gz")):
    rate = float(re.search(r"rate([\d.]+)-", f.name).group(1))
    calls = [json.loads(l) for l in gzip.open(f, "rt")]
    pool = rows(f"rate{rate:g}-pool.csv")
    t0 = min(c["t_start"] for c in calls)
    dur = max(c["t_start"] for c in calls) - t0
    end = max(float(p["epoch"]) for p in pool) - t0
    acq = sorted(c["t_acquired"] - c["t_start"] for c in calls if c["ok"])
    level = []
    for b in range(0, int(end) - 4, 5):
        s = [int(p["pool_ready"]) for p in pool if b <= float(p["epoch"]) - t0 < b + 5]
        if s:
            level.append({"x": b + 2.5, "v": round(sum(s) / len(s), 1),
                          "pend": max(int(p["pods_pending"]) for p in pool if b <= float(p["epoch"]) - t0 < b + 5)})
    refill = []
    for b in range(0, int(end) - 9, 10):
        pooled = sum(1 for c in calls if c["ok"] and b <= c["t_acquired"] - t0 < b + 10 and c["t_acquired"] - c["t_start"] < POOLED)
        r = (pooled + level_at(pool, t0 + b + 10) - level_at(pool, t0 + b)) / 10
        refill.append({"x": b + 5, "v": round(max(r, 0), 2), "pooled": pooled})
    loaded = [r["v"] for r in refill if 10 <= r["x"] - 5 < dur]  # skip the first bin: the pool starts full
    empty = next((float(p["epoch"]) - t0 for p in pool if float(p["epoch"]) > t0 and int(p["pool_ready"]) == 0), None)
    back = next((float(p["epoch"]) - t0 for p in pool if float(p["epoch"]) - t0 > dur and int(p["pool_ready"]) >= 50), None)
    runs.append({"rate": rate, "dur": round(dur), "level": level, "refill": refill})
    metrics.append((f"{rate:g} calls/s for {dur:.0f} s",
                    f"{sum(loaded) / len(loaded):.1f}/s (10 s bins {min(loaded):.1f}–{max(loaded):.1f})",
                    f"empty at {empty:.0f} s" if empty is not None else "never empty",
                    f"{sum(a < POOLED for a in acq) / len(acq):.0%} pooled · acquire p50 {acq[len(acq) // 2]:.2f} s, p95 {acq[int(len(acq) * .95)]:.2f} s",
                    f"{back - dur:.0f} s after arrivals stop" if back else "–"))
runs.sort(key=lambda r: r["rate"])

tr = "".join(f"<tr><td>{a}</td><td class=\"n\">{b}</td><td>{c}</td><td>{e}</td><td>{f}</td></tr>" for a, b, c, e, f in metrics)
html = f"""<h2>How fast the warm pool refills</h2>
<p class="read">Measured separately from the sweeps, on the same cluster, with a pool of 50 polled about once a second. Refill rate is sandboxes the pool gained per second: calls served from the pool plus the change in ready sandboxes, in 10 s bins.</p>
<div class="table-scroll"><table>
  <thead><tr><th>Experiment</th><th class="n">Refill rate</th><th>Pool</th><th>Calls served from the pool</th><th>Back to 50</th></tr></thead>
  <tbody>
    <tr><td>Fill 0 → 50, quiet cluster</td><td class="n">~{50 / fill_s:.0f}/s</td><td>full in {fill_s:.1f} s</td><td>–</td><td>–</td></tr>
    <tr><td>Claim all 50 at once</td><td class="n">~{50 / burst_s:.0f}/s</td><td>lowest {burst_low} ready</td><td>–</td><td>{burst_s:.0f} s after the burst</td></tr>
    {tr}
  </tbody>
</table></div>
<div class="grid2">
  <div><h3 class="sub">Ready sandboxes in the pool (5 s means)</h3><div class="chart" id="c-pool-level"></div><div class="legend" id="l-pool-level"></div></div>
  <div><h3 class="sub">Refill rate (10 s bins)</h3><div class="chart" id="c-refill"></div><div class="legend" id="l-refill"></div></div>
</div>
<ul>
  <li><b>Refill is fast until IPs run out.</b> On a quiet cluster the pool fills at ~{50 / fill_s:.0f} sandboxes/s, and one 50-call burst is replaced within seconds. Under sustained churn it averages ~2.5/s, with dips every ~30 s: each deleted pod's IP is held 30 s before reuse, so new pods wait in Pending for one.</li>
  <li><b>The pool empties fast, then runs near zero.</b> At 3 calls/s it was empty by {runs[0]['level'] and next((r['x'] for r in runs[0]['level'] if r['v'] < 1), 0):.0f} s, but refills nearly match the arrival rate, so most calls still got a pooled sandbox. At 4 calls/s the gap is too wide and acquires fall back to cold starts.</li>
  <li><b>Recovery takes ~30 s</b> after the load stops: the same IP hold, playing out once more.</li>
</ul>
<script>
const REFILL = {json.dumps(runs)};
window.addEventListener("DOMContentLoaded", () => {{
  const series = key => REFILL.map((r, i) => ({{ src: "agent-sandbox-pool50", dash: i > 0, name: `pool 50, ${{r.rate}} calls/s for ${{r.dur}} s`, rows: r[key], run: r }}));
  const xMax = Math.ceil(Math.max(...REFILL.flatMap(r => r.level.map(p => p.x))) / 30) * 30;
  const xs = Array.from({{ length: xMax / 30 + 1 }}, (_, i) => i * 30);
  const stops = (f, yTop) => REFILL.forEach(r => {{
    f.svg.append(el("path", {{ class: "ref", d: `M${{f.x(r.dur)}} ${{f.y(0)}} L${{f.x(r.dur)}} ${{f.y(yTop)}}` }}));
    f.svg.append(el("text", {{ class: "reft", x: f.x(r.dur) + 4, y: f.y(yTop) + 10 }}, `${{r.rate}}/s stops`));
  }});
  let f = frame("c-pool-level", {{ xs, xLabel: "seconds from first arrival", yMax: 50, yLabel: "ready in pool", yTicks: [0, 10, 20, 30, 40, 50] }});
  stops(f, 50);
  lines(f, series("level"), p => p.v, p => `${{p.x - 2.5}}–${{p.x + 2.5}} s`, p => [["ready (mean)", fmt(p.v)], ["pods pending (max)", String(p.pend)]]);
  legend("l-pool-level", series("level"));
  const yMax = niceMax(Math.max(...REFILL.flatMap(r => r.refill.map(p => p.v)), 4));
  f = frame("c-refill", {{ xs, xLabel: "seconds from first arrival", yMax, yLabel: "sandboxes / s", yTicks: ticks(yMax) }});
  stops(f, yMax);
  lines(f, series("refill"), p => p.v, p => `${{p.x - 5}}–${{p.x + 5}} s`, p => [["refill", fmt(p.v) + "/s"], ["served from pool", String(p.pooled)]]);
  legend("l-refill", series("refill"));
}});
</script>
"""
out.write_text(html)
print(f"wrote {out}")
