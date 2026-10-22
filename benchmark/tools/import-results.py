#!/usr/bin/env python3
"""Import a results directory written before the public renames into this repo's format.

    tools/import-results.py SRC_DIR DEST_DIR

Renames cells (closed-manager-w4 -> closed-loop_sandboxmanager_w04, ...) and the
arm recorded in every call (manager -> sandboxmanager, upstream -> agent-sandbox),
gzips each cell's per-call data into DEST_DIR/raw/, and writes summary.jsonl and
summary.tsv. run.log and the harness's .out files are not copied (they hold
cluster-specific details and duplicate the summary).
"""
import gzip, json, re, sys
from pathlib import Path

ARM = {"manager": "sandboxmanager", "upstream": "agent-sandbox", "reuse": "reuse"}


def new_name(old: str, depth: int) -> str:
    m = re.fullmatch(r"(closed|burst|open)-(manager|upstream|reuse)(?:-d(\d+))?-(w|n|r)([\d.]+)", old)
    if not m:
        raise SystemExit(f"unrecognized cell name {old!r}")
    mode, arm, d, kind, val = m.groups()
    d = int(d) if d is not None else depth
    who = {"manager": "sandboxmanager", "reuse": "reuse"}.get(arm) or ("agent-sandbox-cold" if d == 0 else f"agent-sandbox-pool{d}")
    val = f"{int(val):02d}" if kind in "wn" else val
    return f"{ {'closed': 'closed-loop', 'burst': 'burst', 'open': 'open-loop'}[mode] }_{who}_{kind}{val}"


def main(src: Path, dest: Path) -> None:
    (dest / "raw").mkdir(parents=True, exist_ok=True)
    rows = [json.loads(l) for l in open(src / "summary.jsonl")]
    out_rows = []
    for r in rows:
        name = new_name(r["cell"], r["pool_depth"])
        calls = src / f"{r['cell']}.jsonl"
        if calls.exists():
            with open(calls) as f, gzip.open(dest / "raw" / f"{name}.jsonl.gz", "wt") as g:
                for line in f:
                    c = json.loads(line)
                    c["arm"] = ARM.get(c["arm"], c["arm"])
                    g.write(json.dumps(c) + "\n")
        r = dict(r, cell=name, args=re.sub(r"--arm (manager|upstream)", lambda m: "--arm " + ARM[m.group(1)], r.get("args", "")))
        out_rows.append(r)
    with open(dest / "summary.jsonl", "w") as f:
        for r in out_rows:
            f.write(json.dumps(r) + "\n")
    cols = ["cell", "pool_depth", "calls", "errors", "tool_calls_per_s", "acquire_p50", "acquire_p95", "e2e_p50", "e2e_p95", "stopped"]
    with open(dest / "summary.tsv", "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in out_rows:
            f.write("\t".join(str(x) for x in [r["cell"], r["pool_depth"], r.get("calls"), r.get("errors"), r.get("tool_calls_per_s"),
                r.get("acquire_s", {}).get("50"), r.get("acquire_s", {}).get("95"), r.get("e2e_s", {}).get("50"),
                r.get("e2e_s", {}).get("95"), r.get("stopped") or ""]) + "\n")
    print(f"{len(out_rows)} cells -> {dest}")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
